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

// IVM incremental refresh for ARRAY_AGG / COLLECT_LIST aggregate MVs.
// Reading the MV array columns is wrapped in array_sort because element order is not guaranteed;
// array_sort keeps the .out output stable.

suite("test_ivm_agg_array_1") {

    def base = "test_ivm_array_agg_collect_base"
    def aggMv = "test_ivm_array_agg_collect_agg_mv"
    def listMv = "test_ivm_array_agg_collect_list_mv"
    def scalarMv = "test_ivm_array_agg_collect_scalar_mv"

    def refresh = { mv ->
        sql """REFRESH MATERIALIZED VIEW ${mv} INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName(mv)
    }

    // =========================================================
    // Setup: MOW base table with one row per unique id
    // =========================================================

    sql """drop materialized view if exists ${scalarMv};"""
    sql """drop materialized view if exists ${listMv};"""
    sql """drop materialized view if exists ${aggMv};"""
    sql """drop table if exists ${base};"""

    sql """
        CREATE TABLE ${base} (
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

    // Initial rows: k=1 has two values, k=2 has two values, k=3 has one NULL value.
    sql """
        INSERT INTO ${base} VALUES
            (1, 1, 10),
            (2, 1, 20),
            (3, 2, 30),
            (4, 2, 40),
            (5, 3, NULL);
    """

    // =========================================================
    // Part 1: grouped ARRAY_AGG (keeps NULL elements)
    // =========================================================

    sql """
        CREATE MATERIALIZED VIEW ${aggMv}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k, array_agg(v) AS arr FROM ${base} GROUP BY k;
    """

    refresh(aggMv)
    order_qt_agg_initial """SELECT k, array_sort(arr) FROM ${aggMv} ORDER BY k"""
    order_qt_agg_initial_source """SELECT k, array_sort(array_agg(v)) FROM ${base} GROUP BY k ORDER BY k"""

    // Insert a new row into k=1 and a new group k=4.
    sql """INSERT INTO ${base} VALUES (6, 1, 15), (7, 4, 70);"""

    refresh(aggMv)
    order_qt_agg_after_insert """SELECT k, array_sort(arr) FROM ${aggMv} ORDER BY k"""
    order_qt_agg_after_insert_source """SELECT k, array_sort(array_agg(v)) FROM ${base} GROUP BY k ORDER BY k"""

    // Update row id=1 (k=1: 10 -> 15): MOW emits delete + insert for the same id.
    sql """INSERT INTO ${base} VALUES (1, 1, 15);"""

    refresh(aggMv)
    order_qt_agg_after_update """SELECT k, array_sort(arr) FROM ${aggMv} ORDER BY k"""
    order_qt_agg_after_update_source """SELECT k, array_sort(array_agg(v)) FROM ${base} GROUP BY k ORDER BY k"""

    // Delete one row of k=1 and the whole group k=3 (its only row had a NULL value).
    sql """DELETE FROM ${base} WHERE id = 2;"""
    sql """DELETE FROM ${base} WHERE k = 3;"""
    // Update (MOW upsert) id=4's value to NULL in the same window: ARRAY_AGG must drop the old
    // element 40 and keep a NULL element for the new row value.
    sql """INSERT INTO ${base} VALUES (4, 2, NULL);"""
    // Dirty another partition so the incremental refresh picks up the deletes.
    sql """INSERT INTO ${base} VALUES (8, 5, 50);"""

    refresh(aggMv)
    order_qt_agg_after_delete """SELECT k, array_sort(arr) FROM ${aggMv} ORDER BY k"""
    order_qt_agg_after_delete_source """SELECT k, array_sort(array_agg(v)) FROM ${base} GROUP BY k ORDER BY k"""

    // Complete refresh must agree with the incremental result.
    sql """REFRESH MATERIALIZED VIEW ${aggMv} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(aggMv)
    order_qt_agg_after_complete """SELECT k, array_sort(arr) FROM ${aggMv} ORDER BY k"""
    order_qt_agg_after_complete_source """SELECT k, array_sort(array_agg(v)) FROM ${base} GROUP BY k ORDER BY k"""

    // =========================================================
    // Part 2: grouped COLLECT_LIST (skips NULL rows)
    // =========================================================

    sql """
        CREATE MATERIALIZED VIEW ${listMv}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k, collect_list(v) AS lst FROM ${base} GROUP BY k;
    """

    refresh(listMv)
    order_qt_list_initial """SELECT k, array_sort(lst) FROM ${listMv} ORDER BY k"""
    order_qt_list_initial_source """SELECT k, array_sort(collect_list(v)) FROM ${base} GROUP BY k ORDER BY k"""

    // Same change windows as Part 1 (base is currently at the "after delete" state).
    // Inserts a non-NULL value into k=2 and a NULL row into the new group k=6: COLLECT_LIST must
    // skip the NULL row (group k=6 stays an empty list), and the group itself stays alive.
    sql """INSERT INTO ${base} VALUES (9, 2, 55), (10, 6, NULL);"""

    refresh(listMv)
    order_qt_list_after_insert """SELECT k, array_sort(lst) FROM ${listMv} ORDER BY k"""
    order_qt_list_after_insert_source """SELECT k, array_sort(collect_list(v)) FROM ${base} GROUP BY k ORDER BY k"""

    // Delete a non-NULL row of k=2, insert a new non-NULL value, and update (MOW upsert) the
    // remaining row id=9 from 55 to NULL in the same window: COLLECT_LIST must drop 55 and ignore
    // the NULL replacement, keeping the other non-NULL elements.
    sql """DELETE FROM ${base} WHERE id = 3;"""
    sql """INSERT INTO ${base} VALUES (11, 2, 60);"""
    sql """INSERT INTO ${base} VALUES (9, 2, NULL);"""

    refresh(listMv)
    order_qt_list_after_delete """SELECT k, array_sort(lst) FROM ${listMv} ORDER BY k"""
    order_qt_list_after_delete_source """SELECT k, array_sort(collect_list(v)) FROM ${base} GROUP BY k ORDER BY k"""

    // Element-count sanity independent of ordering assumptions.
    order_qt_list_size_after_delete """SELECT k, array_size(lst) FROM ${listMv} ORDER BY k"""
    order_qt_list_size_after_delete_source """
        SELECT k, array_size(collect_list(v)) FROM ${base} GROUP BY k ORDER BY k"""

    // Delete the only row of k=6 (a NULL row, so COLLECT_LIST never saw it) together with an
    // insert on k=2 in the same window to dirty the partition: the empty group disappears while
    // the other lists keep their non-NULL elements.
    sql """DELETE FROM ${base} WHERE id = 10;"""
    sql """INSERT INTO ${base} VALUES (12, 2, 70);"""

    refresh(listMv)
    order_qt_list_after_group_delete """SELECT k, array_sort(lst) FROM ${listMv} ORDER BY k"""
    order_qt_list_after_group_delete_source """
        SELECT k, array_sort(collect_list(v)) FROM ${base} GROUP BY k ORDER BY k"""

    sql """REFRESH MATERIALIZED VIEW ${listMv} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(listMv)
    order_qt_list_after_complete """SELECT k, array_sort(lst) FROM ${listMv} ORDER BY k"""
    order_qt_list_after_complete_source """SELECT k, array_sort(collect_list(v)) FROM ${base} GROUP BY k ORDER BY k"""

    // =========================================================
    // Part 3: scalar ARRAY_AGG over the whole table
    // =========================================================

    sql """
        CREATE MATERIALIZED VIEW ${scalarMv}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT array_agg(v) AS arr FROM ${base};
    """

    refresh(scalarMv)
    order_qt_scalar_after_refresh """SELECT array_sort(arr) FROM ${scalarMv}"""
    order_qt_scalar_after_refresh_source """SELECT array_sort(array_agg(v)) FROM ${base}"""

    // A change window that contains both the last inserts and their deletes leaves the base
    // table empty; the scalar MV must collapse to an empty array (not a NULL).
    sql """INSERT INTO ${base} VALUES (12, 7, 1), (13, 8, 2);"""
    sql """DELETE FROM ${base} WHERE id > 0;"""

    refresh(scalarMv)
    order_qt_scalar_empty_table """SELECT array_sort(arr) FROM ${scalarMv}"""
    order_qt_scalar_empty_table_source """SELECT array_sort(array_agg(v)) FROM ${base}"""

    // Sanity: a later non-empty window still upserts a real array.
    sql """INSERT INTO ${base} VALUES (14, 9, NULL);"""
    refresh(scalarMv)
    order_qt_scalar_back_to_null_row """SELECT array_sort(arr) FROM ${scalarMv}"""
    order_qt_scalar_back_to_null_row_source """SELECT array_sort(array_agg(v)) FROM ${base}"""

    // Final COMPLETE refresh of every part (waitingMTMVTaskFinishedByMvName asserts the task
    // status is SUCCESS) must reproduce the incremental result.
    sql """REFRESH MATERIALIZED VIEW ${scalarMv} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(scalarMv)
    order_qt_scalar_after_final_complete """SELECT array_sort(arr) FROM ${scalarMv}"""
    order_qt_scalar_after_final_complete_source """SELECT array_sort(array_agg(v)) FROM ${base}"""

    // =========================================================
    // Part 4: mixed aggregate MV -- ARRAY_AGG next to COUNT(*) / SUM in one MV
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_array_mixed_mv;"""
    sql """drop table if exists test_ivm_agg_array_mixed_base;"""

    sql """
        CREATE TABLE test_ivm_agg_array_mixed_base (
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

    sql """INSERT INTO test_ivm_agg_array_mixed_base VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, NULL);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_array_mixed_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k, array_agg(v) AS arr, COUNT(*) AS cnt, SUM(v) AS sv
           FROM test_ivm_agg_array_mixed_base GROUP BY k;
    """

    refresh("test_ivm_agg_array_mixed_mv")
    order_qt_mixed_initial """
        SELECT k, array_sort(arr), cnt, sv FROM test_ivm_agg_array_mixed_mv ORDER BY k"""
    order_qt_mixed_initial_source """
        SELECT k, array_sort(array_agg(v)), COUNT(*), SUM(v)
        FROM test_ivm_agg_array_mixed_base GROUP BY k ORDER BY k"""

    // Update id=2 (k=1: 20 -> 15) and insert a new group k=3 in one window: array multiset,
    // COUNT and SUM signed deltas must all merge consistently in the same refresh.
    sql """INSERT INTO test_ivm_agg_array_mixed_base VALUES (2, 1, 15), (5, 3, 40);"""

    refresh("test_ivm_agg_array_mixed_mv")
    order_qt_mixed_after_update """
        SELECT k, array_sort(arr), cnt, sv FROM test_ivm_agg_array_mixed_mv ORDER BY k"""
    order_qt_mixed_after_update_source """
        SELECT k, array_sort(array_agg(v)), COUNT(*), SUM(v)
        FROM test_ivm_agg_array_mixed_base GROUP BY k ORDER BY k"""

    // Delete the NULL row of k=2 in a delete-plus-insert window.
    sql """DELETE FROM test_ivm_agg_array_mixed_base WHERE id = 4;"""
    sql """INSERT INTO test_ivm_agg_array_mixed_base VALUES (6, 3, 50);"""

    refresh("test_ivm_agg_array_mixed_mv")
    order_qt_mixed_after_delete """
        SELECT k, array_sort(arr), cnt, sv FROM test_ivm_agg_array_mixed_mv ORDER BY k"""
    order_qt_mixed_after_delete_source """
        SELECT k, array_sort(array_agg(v)), COUNT(*), SUM(v)
        FROM test_ivm_agg_array_mixed_base GROUP BY k ORDER BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_array_mixed_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_array_mixed_mv")
    order_qt_mixed_after_complete """
        SELECT k, array_sort(arr), cnt, sv FROM test_ivm_agg_array_mixed_mv ORDER BY k"""
    order_qt_mixed_after_complete_source """
        SELECT k, array_sort(array_agg(v)), COUNT(*), SUM(v)
        FROM test_ivm_agg_array_mixed_base GROUP BY k ORDER BY k"""

    sql """drop materialized view if exists test_ivm_agg_array_mixed_mv;"""
    sql """drop table if exists test_ivm_agg_array_mixed_base;"""

    sql """drop materialized view if exists ${scalarMv};"""
    sql """drop materialized view if exists ${listMv};"""
    sql """drop materialized view if exists ${aggMv};"""
    sql """drop table if exists ${base};"""
}
