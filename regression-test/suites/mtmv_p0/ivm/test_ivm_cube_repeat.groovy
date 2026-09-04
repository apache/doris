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

// Regression for DORIS-27304 / apache/doris#65968:
// creating an INCREMENTAL (IVM) materialized view whose definition
// uses GROUP BY CUBE + GROUPING/GROUPING_ID used to fail with
// "Input slot(s) not in child's output: GROUPING_ID#...".
//
// This case builds a single IVM (no COMPLETE reference MV), then applies
// multiple INSERT/UPDATE/DELETE batches on the base table, each followed
// by an INCREMENTAL refresh, and finishes with a COMPLETE refresh. Correctness
// of every stage is pinned by the `.out` files.

suite("test_ivm_cube_repeat") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"
    sql "SET enable_materialized_view_rewrite = false"

    sql """drop materialized view if exists test_ivm_cube_repro_mv;"""
    sql """drop table if exists test_ivm_cube_repro_base;"""

    sql """
        CREATE TABLE test_ivm_cube_repro_base (
            id BIGINT NOT NULL,
            region VARCHAR(16) NOT NULL,
            category VARCHAR(16) NOT NULL,
            amount DECIMAL(20, 4) NOT NULL,
            qty BIGINT NOT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_cube_repro_base VALUES
            (1, 'east', 'book', 10, 1),
            (2, 'east', 'toy', 20, 2),
            (3, 'west', 'book', 30, 3),
            (4, 'west', 'toy', 40, 4);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_cube_repro_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT region, category,
               GROUPING(region) AS g_region,
               GROUPING(category) AS g_category,
               GROUPING_ID(region, category) AS gid,
               COUNT(*) AS row_count,
               SUM(amount) AS total_amount,
               SUM(qty) AS total_qty
        FROM test_ivm_cube_repro_base
        GROUP BY CUBE(region, category);
    """

    // Baseline: first COMPLETE refresh to build the initial snapshot.
    sql """REFRESH MATERIALIZED VIEW test_ivm_cube_repro_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_cube_repro_mv")
    order_qt_cube_baseline """
        SELECT region, category, g_region, g_category, gid, row_count, total_amount, total_qty
        FROM test_ivm_cube_repro_mv
    """

    // Batch 1 (INSERT + DELETE): add id=5 north/book, delete id=3 west/book.
    // INCREMENTAL refresh maintains all four grouping layers.
    sql """INSERT INTO test_ivm_cube_repro_base VALUES (5, 'north', 'book', 50, 5);"""
    sql """DELETE FROM test_ivm_cube_repro_base WHERE id = 3;"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_cube_repro_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_cube_repro_mv")
    order_qt_cube_after_batch1 """
        SELECT region, category, g_region, g_category, gid, row_count, total_amount, total_qty
        FROM test_ivm_cube_repro_mv
    """

    // Batch 2 UPDATE + INSERT: update id=1 amount 10->15, insert id=(6, 'south', 'toy', 60, 6).
    sql """UPDATE test_ivm_cube_repro_base SET amount = 15 WHERE id = 1;"""
    sql """INSERT INTO test_ivm_cube_repro_base VALUES (6, 'south', 'toy', 60, 6);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_cube_repro_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_cube_repro_mv")
    order_qt_cube_after_batch2 """
        SELECT region, category, g_region, g_category, gid, row_count, total_amount, total_qty
        FROM test_ivm_cube_repro_mv
    """

    // Batch 3 DELETE + INSERT: delete id=4 west/toy, insert id=(7, 'east', 'toy', 20, 2).
    sql """DELETE FROM test_ivm_cube_repro_base WHERE id = 4;"""
    sql """INSERT INTO test_ivm_cube_repro_base VALUES (7, 'south', 'toy', 20, 2);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_cube_repro_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_cube_repro_mv")
    order_qt_cube_after_batch3 """
        SELECT region, category, g_region, g_category, gid, row_count, total_amount, total_qty
        FROM test_ivm_cube_repro_mv
    """

    // Batch 4 UPDATE with grouping-key migration: move id=2 from (east, toy) to (north, toy).
    // This touches all four grouping layers: east/toy group disappears (group removal),
    // north/toy group appears, and region/category/overall aggregates are adjusted.
    sql """UPDATE test_ivm_cube_repro_base SET region = 'north' WHERE id = 2;"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_cube_repro_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_cube_repro_mv")
    order_qt_cube_after_batch4 """
        SELECT region, category, g_region, g_category, gid, row_count, total_amount, total_qty
        FROM test_ivm_cube_repro_mv
    """

    // Final COMPLETE refresh must converge to the same result as the last INCREMENTAL state.
    sql """REFRESH MATERIALIZED VIEW test_ivm_cube_repro_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_cube_repro_mv")
    order_qt_cube_after_complete """
        SELECT region, category, g_region, g_category, gid, row_count, total_amount, total_qty
        FROM test_ivm_cube_repro_mv
    """
}