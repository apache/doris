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

// Regression coverage for the IVM defect where a base table's own hidden
// __DORIS_SEQUENCE_COL__ shadowed the delta-allocated sequence. An UPSERT on a
// base table that carries a BIGINT sequence column reported SUCCESS but the
// updated row was left with __DORIS_DELETE_SIGN__ = 1 (the row disappeared),
// and a later INCREMENTAL refresh could not restore it.
//
// Covers the scenarios from the defect:
//   * a single-layer IVM over plain MOW base tables that carry a BIGINT
//     sequence column (INNER and LEFT JOIN);
//   * the plain MOW control without a source sequence;
//   * a cascading IVM (L2 reads L1) where the updated dimension is not read by
//     the upstream MV, and where the upstream and the dimension are updated
//     together;
//   * INSERT / UPDATE / DELETE, several updates in one batch, the LEFT JOIN
//     match/unmatch transition, and a full column comparison against the base
//     tables.

suite("test_ivm_base_sequence_isolation") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"
    // Disable both transparent-rewrite paths: the reference queries below are
    // the MV definition SQL itself and would otherwise be answered from the MV,
    // and the base-table DML would otherwise be redirected into the MV, hiding
    // the defect this suite exists to catch.
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"

    sql "DROP MATERIALIZED VIEW IF EXISTS test_ivm_base_seq_mv_inner"
    sql "DROP MATERIALIZED VIEW IF EXISTS test_ivm_base_seq_mv_left"
    sql "DROP MATERIALIZED VIEW IF EXISTS test_ivm_base_seq_mv_plain"
    sql "DROP MATERIALIZED VIEW IF EXISTS test_ivm_base_seq_mv_l2"
    sql "DROP MATERIALIZED VIEW IF EXISTS test_ivm_base_seq_mv_l1"
    sql "DROP TABLE IF EXISTS test_ivm_base_seq_fact"
    sql "DROP TABLE IF EXISTS test_ivm_base_seq_dim1"
    sql "DROP TABLE IF EXISTS test_ivm_base_seq_dim2"
    sql "DROP TABLE IF EXISTS test_ivm_base_seq_plain"

    // ---- Case D / A / C base tables: plain MOW tables that carry a BIGINT
    // sequence column, so every OlapScan leaks a hidden __DORIS_SEQUENCE_COL__.
    sql """
        CREATE TABLE test_ivm_base_seq_fact (
            id INT NOT NULL,
            amount INT NOT NULL,
            seq BIGINT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true",
            "function_column.sequence_col" = "seq"
        )
    """
    sql """
        CREATE TABLE test_ivm_base_seq_dim1 (
            id INT NOT NULL,
            name VARCHAR(32) NOT NULL,
            seq BIGINT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true",
            "function_column.sequence_col" = "seq"
        )
    """
    sql """
        CREATE TABLE test_ivm_base_seq_dim2 (
            id INT NOT NULL,
            note VARCHAR(32) NOT NULL,
            seq BIGINT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true",
            "function_column.sequence_col" = "seq"
        )
    """
    // ---- Case B control: the same business shape, but no source sequence.
    sql """
        CREATE TABLE test_ivm_base_seq_plain (
            id INT NOT NULL,
            note VARCHAR(32) NOT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """

    sql "INSERT INTO test_ivm_base_seq_fact VALUES (1, 100, 0), (2, 200, 0), (3, 300, 0)"
    sql "INSERT INTO test_ivm_base_seq_dim1 VALUES (1, 'a', 0), (2, 'b', 0), (3, 'c', 0)"
    sql "INSERT INTO test_ivm_base_seq_dim2 VALUES (1, 'a1', 0), (2, 'b1', 0), (3, 'c1', 0)"
    sql "INSERT INTO test_ivm_base_seq_plain VALUES (1, 'a1'), (2, 'b1'), (3, 'c1')"

    // ============================ Case D: single-layer INNER JOIN =============
    // Updating the joined dimension used to leave the updated row at
    // DELETE_SIGN = 1 while the refresh task reported SUCCESS.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_base_seq_mv_inner
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT f.id AS id, f.amount AS amount, d1.name AS name
        FROM test_ivm_base_seq_fact f
        JOIN test_ivm_base_seq_dim1 d1 ON f.id = d1.id
    """
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_inner INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_inner")

    // UPDATE of a joined base table carrying a BIGINT sequence column.
    sql "UPDATE test_ivm_base_seq_dim1 SET name = 'a2' WHERE id = 1"
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_inner INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_inner")
    order_qt_base_seq_inner_after_update """
        SELECT id, amount, name FROM test_ivm_base_seq_mv_inner
    """
    order_qt_base_seq_inner_after_update_reference """
        SELECT f.id AS id, f.amount AS amount, d1.name AS name
        FROM test_ivm_base_seq_fact f
        JOIN test_ivm_base_seq_dim1 d1 ON f.id = d1.id
    """

    // INSERT / UPDATE / DELETE plus several updates in one batch, on the
    // sequence-carrying base tables.
    sql "UPDATE test_ivm_base_seq_dim1 SET name = 'b2' WHERE id = 2"
    sql "UPDATE test_ivm_base_seq_fact SET amount = 111 WHERE id = 3"
    sql "INSERT INTO test_ivm_base_seq_fact VALUES (4, 400, 0)"
    sql "INSERT INTO test_ivm_base_seq_dim1 VALUES (4, 'd', 0)"
    sql "DELETE FROM test_ivm_base_seq_dim1 WHERE id = 3"
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_inner INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_inner")
    order_qt_base_seq_inner_after_batch """
        SELECT id, amount, name FROM test_ivm_base_seq_mv_inner
    """
    order_qt_base_seq_inner_after_batch_reference """
        SELECT f.id AS id, f.amount AS amount, d1.name AS name
        FROM test_ivm_base_seq_fact f
        JOIN test_ivm_base_seq_dim1 d1 ON f.id = d1.id
    """

    // Re-INSERT the deleted key: the row must come back through INCREMENTAL.
    sql "INSERT INTO test_ivm_base_seq_dim1 VALUES (3, 'c2', 0)"
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_inner INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_inner")
    order_qt_base_seq_inner_after_reinsert """
        SELECT id, amount, name FROM test_ivm_base_seq_mv_inner
    """

    // ============================ Case C: single-layer LEFT JOIN ==============
    // Match / unmatch transition: deleting the right side must turn the row
    // into a NULL-extended match, and re-inserting must rematch it.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_base_seq_mv_left
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT f.id AS id, f.amount AS amount, d1.name AS name, d2.note AS note
        FROM test_ivm_base_seq_fact f
        LEFT JOIN test_ivm_base_seq_dim1 d1 ON f.id = d1.id
        LEFT JOIN test_ivm_base_seq_dim2 d2 ON f.id = d2.id
    """
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_left INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_left")

    sql "UPDATE test_ivm_base_seq_dim2 SET note = 'a1x' WHERE id = 1"
    sql "DELETE FROM test_ivm_base_seq_dim2 WHERE id = 2"
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_left INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_left")
    order_qt_base_seq_left_after_unmatch """
        SELECT id, amount, name, note FROM test_ivm_base_seq_mv_left
    """
    order_qt_base_seq_left_after_unmatch_reference """
        SELECT f.id AS id, f.amount AS amount, d1.name AS name, d2.note AS note
        FROM test_ivm_base_seq_fact f
        LEFT JOIN test_ivm_base_seq_dim1 d1 ON f.id = d1.id
        LEFT JOIN test_ivm_base_seq_dim2 d2 ON f.id = d2.id
    """

    sql "INSERT INTO test_ivm_base_seq_dim2 VALUES (2, 'b1y', 0)"
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_left INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_left")
    order_qt_base_seq_left_after_rematch """
        SELECT id, amount, name, note FROM test_ivm_base_seq_mv_left
    """

    // ============================ Case B: plain control (no sequence) =========
    sql """
        CREATE MATERIALIZED VIEW test_ivm_base_seq_mv_plain
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT f.id AS id, f.amount AS amount, p.note AS note
        FROM test_ivm_base_seq_fact f
        JOIN test_ivm_base_seq_plain p ON f.id = p.id
    """
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_plain INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_plain")

    sql "UPDATE test_ivm_base_seq_plain SET note = 'a1z' WHERE id = 1"
    sql "UPDATE test_ivm_base_seq_plain SET note = 'b1z' WHERE id = 2"
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_plain INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_plain")
    order_qt_base_seq_plain_after_update """
        SELECT id, amount, note FROM test_ivm_base_seq_mv_plain
    """
    order_qt_base_seq_plain_after_update_reference """
        SELECT f.id AS id, f.amount AS amount, p.note AS note
        FROM test_ivm_base_seq_fact f
        JOIN test_ivm_base_seq_plain p ON f.id = p.id
    """

    // ============================ Case A: cascading IVM ======================
    // L1 reads fact + dim1; L2 reads L1 (whose snapshot carries L1's own hidden
    // sequence) plus dim2. Updating dim2 -- which L1 does not read -- and
    // refreshing only L2 used to drop the row.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_base_seq_mv_l1
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
        AS
        SELECT f.id AS id, f.amount AS amount, d1.name AS name
        FROM test_ivm_base_seq_fact f
        JOIN test_ivm_base_seq_dim1 d1 ON f.id = d1.id
    """
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_l1 INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_l1")

    sql """
        CREATE MATERIALIZED VIEW test_ivm_base_seq_mv_l2
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT l1.id AS id, l1.amount AS amount, l1.name AS name, d2.note AS note
        FROM test_ivm_base_seq_mv_l1 l1
        JOIN test_ivm_base_seq_dim2 d2 ON l1.id = d2.id
    """
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_l2 COMPLETE"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_l2")

    // Update a dimension that L1 does not read, then refresh only L2.
    sql "UPDATE test_ivm_base_seq_dim2 SET note = 'a1w' WHERE id = 1"
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_l2 INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_l2")
    order_qt_base_seq_l2_dim2_only_update """
        SELECT id, amount, name, note FROM test_ivm_base_seq_mv_l2
    """
    order_qt_base_seq_l2_dim2_only_update_reference """
        SELECT l1.id AS id, l1.amount AS amount, l1.name AS name, d2.note AS note
        FROM test_ivm_base_seq_mv_l1 l1
        JOIN test_ivm_base_seq_dim2 d2 ON l1.id = d2.id
    """

    // Update the upstream and the dimension together, then refresh L1 and L2.
    sql "UPDATE test_ivm_base_seq_dim1 SET name = 'a3' WHERE id = 1"
    sql "UPDATE test_ivm_base_seq_dim2 SET note = 'a1v' WHERE id = 1"
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_l1 INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_l1")
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_l2 INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_l2")
    order_qt_base_seq_l2_both_update """
        SELECT id, amount, name, note FROM test_ivm_base_seq_mv_l2
    """
    order_qt_base_seq_l2_both_update_reference """
        SELECT l1.id AS id, l1.amount AS amount, l1.name AS name, d2.note AS note
        FROM test_ivm_base_seq_mv_l1 l1
        JOIN test_ivm_base_seq_dim2 d2 ON l1.id = d2.id
    """

    // A full COMPLETE refresh must converge to the same result as INCREMENTAL:
    // the same business key with a new value must never keep DELETE_SIGN = 1.
    sql "REFRESH MATERIALIZED VIEW test_ivm_base_seq_mv_inner COMPLETE"
    waitingMTMVTaskFinishedByMvName("test_ivm_base_seq_mv_inner")
    order_qt_base_seq_inner_after_complete """
        SELECT id, amount, name FROM test_ivm_base_seq_mv_inner
    """
}
