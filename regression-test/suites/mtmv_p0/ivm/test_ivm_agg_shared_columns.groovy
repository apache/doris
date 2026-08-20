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

suite("test_ivm_agg_shared_columns") {

    // =========================================================
    // Verify IVM aggregate hidden column sharing via DESC.
    // Visible columns are reused as hidden state when their
    // (agg type, expression) matches, so fewer __DORIS_IVM_AGG_*
    // columns are created than the old per-target scheme.
    // NOTE: set show_hidden_columns=true BEFORE the DESC only —
    // enabling it earlier puts the session in debug mode and
    // blocks CREATE MATERIALIZED VIEW.
    // =========================================================

    // ---------------------------------------------------------
    // Case 1: SUM(x), COUNT(x)
    // SUM needs hidden COUNT(x); the visible COUNT(x) column
    // already provides it -> no extra hidden column besides the
    // group count.
    // ---------------------------------------------------------
    sql """drop materialized view if exists test_ivm_agg_share_mv1;"""
    sql """drop table if exists test_ivm_agg_share_base1;"""

    sql """
        CREATE TABLE test_ivm_agg_share_base1 (
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

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_share_mv1
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, SUM(v1) AS sum_v1, COUNT(v1) AS cnt_v1 FROM test_ivm_agg_share_base1 GROUP BY k1;
    """

    sql """set show_hidden_columns=true"""
    qt_share_mv1_desc """DESC test_ivm_agg_share_mv1"""

    sql """set show_hidden_columns=false"""

    // ---------------------------------------------------------
    // Case 2: SUM(x), AVG(x), COUNT(x)
    // AVG needs SUM(x) and COUNT(x); both visible columns exist,
    // SUM needs COUNT(x) (visible) -> minimal hidden columns.
    // ---------------------------------------------------------
    sql """drop materialized view if exists test_ivm_agg_share_mv2;"""
    sql """drop table if exists test_ivm_agg_share_base2;"""

    sql """
        CREATE TABLE test_ivm_agg_share_base2 (
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

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_share_mv2
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, SUM(v1) AS sum_v1, AVG(v1) AS avg_v1, COUNT(v1) AS cnt_v1
           FROM test_ivm_agg_share_base2 GROUP BY k1;
    """

    sql """set show_hidden_columns=true"""
    qt_share_mv2_desc """DESC test_ivm_agg_share_mv2"""

    sql """set show_hidden_columns=false"""

    // ---------------------------------------------------------
    // Case 3: AVG(x), SUM(x) (reversed order)
    // AVG needs SUM(x) and COUNT(x); COUNT(x) has no visible
    // column, so hidden SUM and COUNT are created; SUM(v1)
    // reuses the hidden SUM.
    // ---------------------------------------------------------
    sql """drop materialized view if exists test_ivm_agg_share_mv3;"""
    sql """drop table if exists test_ivm_agg_share_base3;"""

    sql """
        CREATE TABLE test_ivm_agg_share_base3 (
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

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_share_mv3
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, AVG(v1) AS avg_v1, SUM(v1) AS sum_v1 FROM test_ivm_agg_share_base3 GROUP BY k1;
    """

    sql """set show_hidden_columns=true"""
    qt_share_mv3_desc """DESC test_ivm_agg_share_mv3"""

    sql """set show_hidden_columns=false"""

    // ---------------------------------------------------------
    // Case 4: BITMAP_UNION(y), BITMAP_UNION_COUNT(y)
    // BITMAP_UNION_COUNT needs hidden BITMAP_UNION(y); the visible
    // BITMAP_UNION(y) column provides it -> no extra hidden
    // column besides group count.
    // ---------------------------------------------------------
    sql """drop materialized view if exists test_ivm_agg_share_mv4;"""
    sql """drop table if exists test_ivm_agg_share_base4;"""

    sql """
        CREATE TABLE test_ivm_agg_share_base4 (
            k1 INT,
            y BITMAP
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
        CREATE MATERIALIZED VIEW test_ivm_agg_share_mv4
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, BITMAP_UNION(y) AS bu_y, BITMAP_UNION_COUNT(y) AS bc_y
           FROM test_ivm_agg_share_base4 GROUP BY k1;
    """

    sql """set show_hidden_columns=true"""
    qt_share_mv4_desc """DESC test_ivm_agg_share_mv4"""

    sql """set show_hidden_columns=false"""

    // ---------------------------------------------------------
    // Sanity: MVs refresh incrementally and completely, verifying
    // the shared hidden columns produce correct results.
    // ---------------------------------------------------------
    sql """INSERT INTO test_ivm_agg_share_base1 VALUES (1, 10), (2, 20);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv1 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv1")
    order_qt_share_mv1 """SELECT k1, sum_v1, cnt_v1 FROM test_ivm_agg_share_mv1 ORDER BY k1"""

    // Update k1=1 and insert a new group k1=3, then incremental refresh.
    sql """INSERT INTO test_ivm_agg_share_base1 VALUES (1, 15), (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv1 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv1")
    order_qt_share_mv1_incr """SELECT k1, sum_v1, cnt_v1 FROM test_ivm_agg_share_mv1 ORDER BY k1"""

    // Complete refresh should match the incremental result.
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv1 COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv1")
    order_qt_share_mv1_complete """SELECT k1, sum_v1, cnt_v1 FROM test_ivm_agg_share_mv1 ORDER BY k1"""

    sql """INSERT INTO test_ivm_agg_share_base2 VALUES (1, 10), (2, 20);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv2 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv2")
    order_qt_share_mv2 """SELECT k1, sum_v1, avg_v1, cnt_v1 FROM test_ivm_agg_share_mv2 ORDER BY k1"""

    sql """INSERT INTO test_ivm_agg_share_base2 VALUES (1, 15), (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv2 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv2")
    order_qt_share_mv2_incr """SELECT k1, sum_v1, avg_v1, cnt_v1 FROM test_ivm_agg_share_mv2 ORDER BY k1"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv2 COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv2")
    order_qt_share_mv2_complete """SELECT k1, sum_v1, avg_v1, cnt_v1 FROM test_ivm_agg_share_mv2 ORDER BY k1"""

    sql """INSERT INTO test_ivm_agg_share_base3 VALUES (1, 10), (2, 20);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv3 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv3")
    order_qt_share_mv3 """SELECT k1, avg_v1, sum_v1 FROM test_ivm_agg_share_mv3 ORDER BY k1"""

    sql """INSERT INTO test_ivm_agg_share_base3 VALUES (1, 15), (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv3 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv3")
    order_qt_share_mv3_incr """SELECT k1, avg_v1, sum_v1 FROM test_ivm_agg_share_mv3 ORDER BY k1"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv3 COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv3")
    order_qt_share_mv3_complete """SELECT k1, avg_v1, sum_v1 FROM test_ivm_agg_share_mv3 ORDER BY k1"""

    sql """INSERT INTO test_ivm_agg_share_base4 VALUES (1, to_bitmap(10)), (2, to_bitmap(20));"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv4 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv4")
    order_qt_share_mv4 """SELECT k1, bitmap_count(bu_y), bc_y FROM test_ivm_agg_share_mv4 ORDER BY k1"""

    // Insert a new group only (no upsert), keeping the bitmap delete guard out of scope.
    sql """INSERT INTO test_ivm_agg_share_base4 VALUES (3, to_bitmap(30)), (4, to_bitmap(40));"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv4 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv4")
    order_qt_share_mv4_incr """SELECT k1, bitmap_count(bu_y), bc_y FROM test_ivm_agg_share_mv4 ORDER BY k1"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_share_mv4 COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_share_mv4")
    order_qt_share_mv4_complete """SELECT k1, bitmap_count(bu_y), bc_y FROM test_ivm_agg_share_mv4 ORDER BY k1"""
}
