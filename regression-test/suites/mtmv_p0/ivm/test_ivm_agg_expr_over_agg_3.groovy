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

suite("test_ivm_agg_expr_over_agg_3") {
    // Transparent rewrite could answer a query from one of the views under test, which would stop the
    // base-table comparisons in this suite from being an oracle.
    sql """set enable_materialized_view_rewrite = false"""

    // =========================================================
    // Array and bitmap aggregate state under a wrapping
    // expression. Each of these keeps its whole aggregate state
    // in the visible column, which the outer expression drops, so
    // the state is carried by a materialized column; the array
    // cases also compare element contents rather than counts, and
    // ARRAY_AGG keeps NULL elements where COLLECT_LIST skips them.
    // =========================================================

    def refreshIncremental = { mv ->
        sql """REFRESH MATERIALIZED VIEW ${mv} INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName(mv)
    }

    sql """drop materialized view if exists test_ivm_expr_over_agg_3_array_agg;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_3_collect_list;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_3_bitmap_union;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_3_bitmap_union_count;"""
    sql """drop table if exists test_ivm_expr_over_agg_3_base;"""

    sql """
        CREATE TABLE test_ivm_expr_over_agg_3_base (
            id INT,
            k INT,
            v INT
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
    sql """INSERT INTO test_ivm_expr_over_agg_3_base VALUES
        (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, NULL);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_3_array_agg
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, ARRAY_SORT(ARRAY_AGG(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_array_agg_desc """DESC test_ivm_expr_over_agg_3_array_agg"""
    sql """set show_hidden_columns=false"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_3_collect_list
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, ARRAY_SORT(COLLECT_LIST(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_collect_list_desc """DESC test_ivm_expr_over_agg_3_collect_list"""
    sql """set show_hidden_columns=false"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_3_bitmap_union
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, BITMAP_COUNT(BITMAP_UNION(TO_BITMAP(v))) AS b FROM test_ivm_expr_over_agg_3_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_bitmap_union_desc """DESC test_ivm_expr_over_agg_3_bitmap_union"""
    sql """set show_hidden_columns=false"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_3_bitmap_union_count
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, BITMAP_UNION_COUNT(TO_BITMAP(v)) + 0 AS b FROM test_ivm_expr_over_agg_3_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_bitmap_union_count_desc """DESC test_ivm_expr_over_agg_3_bitmap_union_count"""
    sql """set show_hidden_columns=false"""

    // Initial window: all four views, the bitmaps included.
    refreshIncremental("test_ivm_expr_over_agg_3_array_agg")
    refreshIncremental("test_ivm_expr_over_agg_3_collect_list")
    refreshIncremental("test_ivm_expr_over_agg_3_bitmap_union")
    refreshIncremental("test_ivm_expr_over_agg_3_bitmap_union_count")
    order_qt_initial_array_agg """SELECT k, lst FROM test_ivm_expr_over_agg_3_array_agg"""
    order_qt_initial_array_agg_source """
        SELECT k, ARRAY_SORT(ARRAY_AGG(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
    order_qt_initial_collect_list """SELECT k, lst FROM test_ivm_expr_over_agg_3_collect_list"""
    order_qt_initial_collect_list_source """
        SELECT k, ARRAY_SORT(COLLECT_LIST(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
    order_qt_initial_bitmap_union """SELECT k, b FROM test_ivm_expr_over_agg_3_bitmap_union"""
    order_qt_initial_bitmap_union_source """
        SELECT k, BITMAP_COUNT(BITMAP_UNION(TO_BITMAP(v))) AS b FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
    order_qt_initial_bitmap_union_count """SELECT k, b FROM test_ivm_expr_over_agg_3_bitmap_union_count"""
    order_qt_initial_bitmap_union_count_source """
        SELECT k, BITMAP_UNION_COUNT(TO_BITMAP(v)) + 0 AS b FROM test_ivm_expr_over_agg_3_base GROUP BY k"""

    // Insert-only window: a bitmap element cannot be removed incrementally, so the bitmaps are refreshed
    // here (deleting a non-NULL element is the separate limitation covered by test_ivm_bitmap_runtime_fallback).
    sql """INSERT INTO test_ivm_expr_over_agg_3_base VALUES (6, 1, 30), (7, 2, 35);"""
    refreshIncremental("test_ivm_expr_over_agg_3_array_agg")
    refreshIncremental("test_ivm_expr_over_agg_3_collect_list")
    refreshIncremental("test_ivm_expr_over_agg_3_bitmap_union")
    refreshIncremental("test_ivm_expr_over_agg_3_bitmap_union_count")
    order_qt_after_insert_array_agg """SELECT k, lst FROM test_ivm_expr_over_agg_3_array_agg"""
    order_qt_after_insert_array_agg_source """
        SELECT k, ARRAY_SORT(ARRAY_AGG(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
    order_qt_after_insert_collect_list """SELECT k, lst FROM test_ivm_expr_over_agg_3_collect_list"""
    order_qt_after_insert_collect_list_source """
        SELECT k, ARRAY_SORT(COLLECT_LIST(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
    order_qt_after_insert_bitmap_union """SELECT k, b FROM test_ivm_expr_over_agg_3_bitmap_union"""
    order_qt_after_insert_bitmap_union_source """
        SELECT k, BITMAP_COUNT(BITMAP_UNION(TO_BITMAP(v))) AS b FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
    order_qt_after_insert_bitmap_union_count """SELECT k, b FROM test_ivm_expr_over_agg_3_bitmap_union_count"""
    order_qt_after_insert_bitmap_union_count_source """
        SELECT k, BITMAP_UNION_COUNT(TO_BITMAP(v)) + 0 AS b FROM test_ivm_expr_over_agg_3_base GROUP BY k"""

    // Equal-count update: the array contents change without the element count changing.
    sql """INSERT INTO test_ivm_expr_over_agg_3_base VALUES (2, 1, 25);"""
    refreshIncremental("test_ivm_expr_over_agg_3_array_agg")
    refreshIncremental("test_ivm_expr_over_agg_3_collect_list")
    order_qt_after_update_array_agg """SELECT k, lst FROM test_ivm_expr_over_agg_3_array_agg"""
    order_qt_after_update_array_agg_source """
        SELECT k, ARRAY_SORT(ARRAY_AGG(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
    order_qt_after_update_collect_list """SELECT k, lst FROM test_ivm_expr_over_agg_3_collect_list"""
    order_qt_after_update_collect_list_source """
        SELECT k, ARRAY_SORT(COLLECT_LIST(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k"""

    // Delete the middle value of k=2 and add one to k=1, then refresh the arrays again.
    sql """DELETE FROM test_ivm_expr_over_agg_3_base WHERE id = 7;"""
    sql """INSERT INTO test_ivm_expr_over_agg_3_base VALUES (8, 1, 40);"""
    refreshIncremental("test_ivm_expr_over_agg_3_array_agg")
    refreshIncremental("test_ivm_expr_over_agg_3_collect_list")
    order_qt_after_delete_array_agg """SELECT k, lst FROM test_ivm_expr_over_agg_3_array_agg"""
    order_qt_after_delete_array_agg_source """
        SELECT k, ARRAY_SORT(ARRAY_AGG(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
    order_qt_after_delete_collect_list """SELECT k, lst FROM test_ivm_expr_over_agg_3_collect_list"""
    order_qt_after_delete_collect_list_source """
        SELECT k, ARRAY_SORT(COLLECT_LIST(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k"""

    // Every view is also completely refreshable and agrees with its source query.
    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_3_array_agg COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_3_array_agg")
    order_qt_complete_array_agg """SELECT k, lst FROM test_ivm_expr_over_agg_3_array_agg"""
    order_qt_complete_array_agg_source """
        SELECT k, ARRAY_SORT(ARRAY_AGG(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_3_collect_list COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_3_collect_list")
    order_qt_complete_collect_list """SELECT k, lst FROM test_ivm_expr_over_agg_3_collect_list"""
    order_qt_complete_collect_list_source """
        SELECT k, ARRAY_SORT(COLLECT_LIST(v)) AS lst FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_3_bitmap_union COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_3_bitmap_union")
    order_qt_complete_bitmap_union """SELECT k, b FROM test_ivm_expr_over_agg_3_bitmap_union"""
    order_qt_complete_bitmap_union_source """
        SELECT k, BITMAP_COUNT(BITMAP_UNION(TO_BITMAP(v))) AS b FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_3_bitmap_union_count COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_3_bitmap_union_count")
    order_qt_complete_bitmap_union_count """SELECT k, b FROM test_ivm_expr_over_agg_3_bitmap_union_count"""
    order_qt_complete_bitmap_union_count_source """
        SELECT k, BITMAP_UNION_COUNT(TO_BITMAP(v)) + 0 AS b FROM test_ivm_expr_over_agg_3_base GROUP BY k"""
}
