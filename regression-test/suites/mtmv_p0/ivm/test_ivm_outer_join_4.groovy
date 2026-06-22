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

suite("test_ivm_outer_join_4") {

    // =========================================================
    // Part 1: FULL OUTER JOIN incremental refresh under the current mock-full-scan delta.
    // Covers unmatched rows retained by either side. When a later row makes an old
    // null-side row matched, the mock delta cannot retract that old row because it
    // reads the current base table instead of a real binlog stream/pre-refresh snapshot.
    // =========================================================
    sql """drop materialized view if exists test_ivm_outer_join_4_mv;"""
    sql """drop table if exists test_ivm_outer_join_4_l;"""
    sql """drop table if exists test_ivm_outer_join_4_r;"""

    sql """
        CREATE TABLE test_ivm_outer_join_4_l (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_outer_join_4_r (
            k1 INT,
            v2 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO test_ivm_outer_join_4_l VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO test_ivm_outer_join_4_r VALUES (2, 200), (3, 300);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_outer_join_4_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS
        SELECT
            test_ivm_outer_join_4_l.k1 AS left_k1,
            test_ivm_outer_join_4_r.k1 AS right_k1,
            test_ivm_outer_join_4_l.v1 AS left_v1,
            test_ivm_outer_join_4_r.v2 AS right_v2
        FROM test_ivm_outer_join_4_l
        FULL OUTER JOIN test_ivm_outer_join_4_r
            ON test_ivm_outer_join_4_l.k1 = test_ivm_outer_join_4_r.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_outer_join_4_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_outer_join_4_mv")
    order_qt_after_complete """
        SELECT left_k1, right_k1, left_v1, right_v2
        FROM test_ivm_outer_join_4_mv
        ORDER BY IFNULL(left_k1, right_k1), left_k1, right_k1, left_v1, right_v2
    """

    // Left delta is unmatched, so the full outer join retains the left row with
    // right columns filled as NULL.
    sql """INSERT INTO test_ivm_outer_join_4_l VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_outer_join_4_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_outer_join_4_mv")
    order_qt_after_left_unmatched_incremental """
        SELECT left_k1, right_k1, left_v1, right_v2
        FROM test_ivm_outer_join_4_mv
        ORDER BY IFNULL(left_k1, right_k1), left_k1, right_k1, left_v1, right_v2
    """

    // Right delta matches an old left-only row. Under the current mock-full-scan
    // delta, the matched full row is added while the old left-only row remains.
    sql """INSERT INTO test_ivm_outer_join_4_r VALUES (1, 100);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_outer_join_4_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_outer_join_4_mv")
    order_qt_after_right_matches_left_incremental """
        SELECT left_k1, right_k1, left_v1, right_v2
        FROM test_ivm_outer_join_4_mv
        ORDER BY IFNULL(left_k1, right_k1), left_k1, right_k1, left_v1, right_v2
    """

    // Right delta is unmatched, so the full outer join retains the right row with
    // left columns filled as NULL.
    sql """INSERT INTO test_ivm_outer_join_4_r VALUES (5, 500);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_outer_join_4_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_outer_join_4_mv")
    order_qt_after_right_unmatched_incremental """
        SELECT left_k1, right_k1, left_v1, right_v2
        FROM test_ivm_outer_join_4_mv
        ORDER BY IFNULL(left_k1, right_k1), left_k1, right_k1, left_v1, right_v2
    """

    // Left delta matches an old right-only row. This is the symmetric full outer
    // join case under the same mock-full-scan delta limitation.
    sql """INSERT INTO test_ivm_outer_join_4_l VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_outer_join_4_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_outer_join_4_mv")
    order_qt_after_left_matches_right_incremental """
        SELECT left_k1, right_k1, left_v1, right_v2
        FROM test_ivm_outer_join_4_mv
        ORDER BY IFNULL(left_k1, right_k1), left_k1, right_k1, left_v1, right_v2
    """
}
