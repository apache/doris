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

// An IVM base table may carry a DATETIME sequence column
// (function_column.sequence_col = a DATETIME column). The base table's hidden
// __DORIS_SEQUENCE_COL__ is therefore DATETIMEV2. The incremental rewrite asks
// for the type's default value when the snapshot side does not produce the
// column, and that request used to fail with "no default value for IVM hidden
// slot: __DORIS_SEQUENCE_COL__, type=DATETIMEV2(6)", so the refresh could not
// be planned at all. The plain join also covers the source-sequence shadowing
// fixed separately: the joined base table's hidden sequence must not be bound
// to the MV's BIGINT sequence column.
suite("test_ivm_datetime_sequence") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"
    // Read the MV's own result below, and keep base-table DML on the base tables,
    // so the incremental path this suite exists to exercise is actually used.
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"

    sql "DROP MATERIALIZED VIEW IF EXISTS mv_ivm_datetime_sequence_plain"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_ivm_datetime_sequence"
    sql "DROP TABLE IF EXISTS t_ivm_datetime_sequence_dim"
    sql "DROP TABLE IF EXISTS t_ivm_datetime_sequence_base"

    sql """
        CREATE TABLE t_ivm_datetime_sequence_base (
            k1 INT NOT NULL,
            v1 INT NULL,
            seq_dt DATETIME(6) NULL
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "function_column.sequence_col" = "seq_dt"
        )
    """
    sql """
        CREATE TABLE t_ivm_datetime_sequence_dim (
            k1 INT NOT NULL,
            name VARCHAR(32) NULL
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """
        INSERT INTO t_ivm_datetime_sequence_base VALUES
            (1, 10, '2026-01-01 00:00:00.000000'),
            (2, 20, '2026-01-01 00:00:00.000000'),
            (3, 30, '2026-01-01 00:00:00.000000')
    """
    sql """
        INSERT INTO t_ivm_datetime_sequence_dim VALUES
            (1, 'a'), (2, 'b'), (3, 'c')
    """

    sql """
        CREATE MATERIALIZED VIEW mv_ivm_datetime_sequence
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT a.k1 AS k1, COUNT(*) AS cnt, SUM(a.v1) AS total
           FROM t_ivm_datetime_sequence_dim b
           JOIN t_ivm_datetime_sequence_base a ON b.k1 = a.k1
           GROUP BY a.k1
    """
    sql """
        CREATE MATERIALIZED VIEW mv_ivm_datetime_sequence_plain
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT a.k1 AS k1, a.v1 AS v1, b.name AS name
           FROM t_ivm_datetime_sequence_dim b
           JOIN t_ivm_datetime_sequence_base a ON b.k1 = a.k1
    """
    sql "REFRESH MATERIALIZED VIEW mv_ivm_datetime_sequence COMPLETE"
    waitingMTMVTaskFinishedByMvName("mv_ivm_datetime_sequence")
    sql "REFRESH MATERIALIZED VIEW mv_ivm_datetime_sequence_plain COMPLETE"
    waitingMTMVTaskFinishedByMvName("mv_ivm_datetime_sequence_plain")
    order_qt_after_build """SELECT k1, cnt, total FROM mv_ivm_datetime_sequence"""
    order_qt_plain_after_build """SELECT k1, v1, name FROM mv_ivm_datetime_sequence_plain"""

    // A delta on the left dimension makes the sequence-bearing base table (the
    // right child) the pre-snapshot side of the join. That is the path where the
    // stream scan drops the DATETIME sequence hidden slot and its default value
    // has to be supplied.
    sql "INSERT INTO t_ivm_datetime_sequence_dim VALUES (1, 'a2')"
    sql "REFRESH MATERIALIZED VIEW mv_ivm_datetime_sequence INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("mv_ivm_datetime_sequence")
    sql "REFRESH MATERIALIZED VIEW mv_ivm_datetime_sequence_plain INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("mv_ivm_datetime_sequence_plain")
    order_qt_after_dim_update """SELECT k1, cnt, total FROM mv_ivm_datetime_sequence"""
    order_qt_plain_after_dim_update """SELECT k1, v1, name FROM mv_ivm_datetime_sequence_plain"""

    // A delta on the base table, ordered by its DATETIME sequence column.
    sql """
        INSERT INTO t_ivm_datetime_sequence_base VALUES
            (2, 22, '2026-01-02 00:00:00.000000')
    """
    sql "REFRESH MATERIALIZED VIEW mv_ivm_datetime_sequence INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("mv_ivm_datetime_sequence")
    sql "REFRESH MATERIALIZED VIEW mv_ivm_datetime_sequence_plain INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("mv_ivm_datetime_sequence_plain")
    order_qt_after_base_update """SELECT k1, cnt, total FROM mv_ivm_datetime_sequence"""
    order_qt_plain_after_base_update """SELECT k1, v1, name FROM mv_ivm_datetime_sequence_plain"""

    // A new key exercises the join's match path with the datetime sequence.
    sql """
        INSERT INTO t_ivm_datetime_sequence_base VALUES
            (4, 40, '2026-01-03 00:00:00.000000')
    """
    sql "INSERT INTO t_ivm_datetime_sequence_dim VALUES (4, 'd')"
    sql "REFRESH MATERIALIZED VIEW mv_ivm_datetime_sequence INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("mv_ivm_datetime_sequence")
    sql "REFRESH MATERIALIZED VIEW mv_ivm_datetime_sequence_plain INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("mv_ivm_datetime_sequence_plain")
    order_qt_after_new_key """SELECT k1, cnt, total FROM mv_ivm_datetime_sequence"""
    order_qt_plain_after_new_key """SELECT k1, v1, name FROM mv_ivm_datetime_sequence_plain"""
}
