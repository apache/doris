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

suite("test_ivm_basic_mtmv") {
    sql """drop materialized view if exists mv_ivm_basic;"""
    sql """drop table if exists t_ivm_basic_base;"""

    // 1. Create base table (MOW — UNIQUE_KEYS with merge-on-write)
    //    IVM generates row_id = hash(unique_keys) for MOW base tables,
    //    which is deterministic across refreshes, so the mock
    //    (reading full base table) correctly upserts existing rows.
    sql """
        CREATE TABLE t_ivm_basic_base (
            k1 INT,
            v1 INT,
            v2 VARCHAR(50)
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

    // 2. Insert initial rows
    sql """
        INSERT INTO t_ivm_basic_base VALUES
            (1, 10, 'aaa'),
            (2, 20, 'bbb'),
            (3, 30, 'ccc');
    """

    // 3. Create IVM materialized view (BUILD DEFERRED, REFRESH INCREMENTAL, ON MANUAL)
    sql """
        CREATE MATERIALIZED VIEW mv_ivm_basic
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM t_ivm_basic_base;
    """

    // 4. Verify MV metadata — state should be INIT (not yet refreshed)
    def mvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'mv_ivm_basic'"""
    logger.info("mv_infos after create: " + mvInfos.toString())
    assertTrue(mvInfos.toString().contains("INIT"))

    // 5. Verify MV is UNIQUE_KEYS with MOW (via desc command)
    def descResult = sql """desc mv_ivm_basic all"""
    logger.info("desc mv: " + descResult.toString())
    assertTrue(descResult.toString().contains("UNIQUE_KEYS"))

    // 6. First COMPLETE refresh (full overwrite, skips IVM path)
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic")
    advance_ivm_stream_offset("mv_ivm_basic")

    // 7. Verify data after first refresh (exclude __IVM_ROW_ID__ column)
    order_qt_after_first_refresh """SELECT k1, v1, v2 FROM mv_ivm_basic"""

    // 8. Insert more rows into base table
    sql """
        INSERT INTO t_ivm_basic_base VALUES
            (4, 40, 'ddd'),
            (5, 50, 'eee');
    """

    // 9. Second refresh via IVM incremental path (mock reads full base table,
    //    deterministic row_id upserts correctly for MOW base table)
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic")

    order_qt_after_second_refresh """SELECT k1, v1, v2 FROM mv_ivm_basic"""

    // 10. Update existing rows in base table
    sql """
        INSERT INTO t_ivm_basic_base VALUES
            (2, 22, 'bbb_updated'),
            (3, 33, 'ccc_updated');
    """

    // 11. Third refresh via IVM incremental — should reflect updated rows
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic")

    order_qt_after_third_refresh """SELECT k1, v1, v2 FROM mv_ivm_basic"""

    // 12. Complete refresh after incremental — should produce same result (full overwrite)
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic")

    order_qt_after_complete_refresh """SELECT k1, v1, v2 FROM mv_ivm_basic"""

    // =========================================================
    // Part 2: Simple scan MV with binlog_op column (dml_factor from binlog_op)
    // =========================================================
    // When the base table has a `binlog_op` column (TinyInt, 0 = insert, 1 = delete),
    // IvmSimpleScanDeltaStrategy derives dml_factor = IF(binlog_op = 0, 1, -1) instead
    // of the literal 1. This means during INCREMENTAL refresh, rows with binlog_op=1
    // receive delete_sign=1 and are removed from the MV.
    //
    // NOTE: INCREMENTAL refresh only triggers when the MV partition is stale
    // (base table has been modified since last refresh). After a COMPLETE refresh
    // with no subsequent base table changes, INCREMENTAL skips with NOT_REFRESH.
    // Therefore, to test op-based dml_factor via INCREMENTAL, we must insert data
    // into the base table first to dirty the partition.

    sql """drop materialized view if exists mv_ivm_basic_op;"""
    sql """drop table if exists t_ivm_basic_op_base;"""

    // Base table with an explicit `binlog_op` column (TinyInt)
    sql """
        CREATE TABLE t_ivm_basic_op_base (
            k1 INT,
            v1 INT,
            v2 VARCHAR(50)
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

    // Insert rows: binlog_op=0 means insert, =1 means delete
    sql """
        INSERT INTO t_ivm_basic_op_base VALUES
            (1, 10, 'aaa'),
            (2, 20, 'bbb');
    """
    sql """DELETE FROM t_ivm_basic_op_base WHERE k1 = 3;"""

    // Create IVM MV (BUILD DEFERRED so no immediate refresh)
    sql """
        CREATE MATERIALIZED VIEW mv_ivm_basic_op
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM t_ivm_basic_op_base;
    """

    // Verify MV state
    def opMvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'mv_ivm_basic_op'"""
    assertTrue(opMvInfos.toString().contains("INIT"))

    // Step 1: COMPLETE refresh sees the current base-table snapshot.
    // Only k1=1 and k1=2 exist at this point.
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic_op COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic_op")
    advance_ivm_stream_offset("mv_ivm_basic_op")

    order_qt_op_after_complete """SELECT k1, v1, v2 FROM mv_ivm_basic_op"""

    // Step 2: Insert a new row (binlog_op=0) to dirty the partition, then INCREMENTAL refresh.
    // The mock delta reads all 4 rows. The dml_factor logic:
    //   k1=1 (binlog_op=0): dml_factor=1  → delete_sign=0 → kept
    //   k1=2 (binlog_op=0): dml_factor=1  → delete_sign=0 → kept
    //   k1=3 (binlog_op=1): dml_factor=-1 → delete_sign=1 → deleted (hidden)
    //   k1=4 (binlog_op=0): dml_factor=1  → delete_sign=0 → kept
    sql """INSERT INTO t_ivm_basic_op_base VALUES (4, 40, 'ddd');"""

    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic_op INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic_op")

    // Only binlog_op=0 rows survive: k1=1, k1=2, k1=4 (k1=3 is deleted)
    order_qt_op_after_incremental """SELECT k1, v1, v2 FROM mv_ivm_basic_op"""

    // Step 3: COMPLETE refresh sees the current base-table snapshot again.
    // k1=3 still does not exist physically, so the result remains k1=1,2,4.
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic_op COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic_op")
    advance_ivm_stream_offset("mv_ivm_basic_op")

    order_qt_op_after_restore """SELECT k1, v1, v2 FROM mv_ivm_basic_op"""

    // Step 4: Upsert k1=3 to binlog_op=0 (was 1), then INCREMENTAL refresh.
    // Now all rows have binlog_op=0, so all survive.
    sql """INSERT INTO t_ivm_basic_op_base VALUES (3, 30, 'ccc');"""

    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic_op INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic_op")

    order_qt_op_after_upsert_incremental """SELECT k1, v1, v2 FROM mv_ivm_basic_op"""

    // =========================================================
    // Part 3: Simple MV with Project -> Filter -> Scan + binlog_op (delete rows filtered)
    // =========================================================
    // Tests that the IVM delta rewrite correctly propagates dml_factor through a Filter node.
    // The MV definition is: SELECT k1, v1 FROM base WHERE v1 > 15
    // When binlog_op=1 rows pass the filter, they should receive delete_sign=1 during INCREMENTAL
    // and be removed from the MV.

    sql """drop materialized view if exists mv_ivm_basic_filter;"""
    sql """drop table if exists t_ivm_basic_filter_base;"""

    sql """
        CREATE TABLE t_ivm_basic_filter_base (
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

    // Insert rows: k1=1(v1=10, op=0) filtered out by WHERE v1 > 15
    //              k1=2(v1=20, op=0) passes filter
    //              k1=3(v1=30, op=1) passes filter but is a "delete" row
    //              k1=4(v1=40, op=0) passes filter
    sql """
        INSERT INTO t_ivm_basic_filter_base VALUES
            (1, 10),
            (2, 20),
            (4, 40);
    """
    sql """DELETE FROM t_ivm_basic_filter_base WHERE k1 = 3;"""

    sql """
        CREATE MATERIALIZED VIEW mv_ivm_basic_filter
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, v1 FROM t_ivm_basic_filter_base WHERE v1 > 15;
    """

    def filterMvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'mv_ivm_basic_filter'"""
    assertTrue(filterMvInfos.toString().contains("INIT"))

    // Step 1: COMPLETE refresh sees the current base-table snapshot after the filter.
    // Visible: k1=2(v1=20), k1=4(v1=40) — note k1=1(v1=10) filtered out and k1=3 does not exist physically.
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic_filter COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic_filter")
    advance_ivm_stream_offset("mv_ivm_basic_filter")

    order_qt_filter_after_complete """SELECT k1, v1 FROM mv_ivm_basic_filter"""

    // Step 2: Insert a new row that passes the filter (dirty the partition), then INCREMENTAL.
    // Mock delta reads all 5 rows. After filter v1 > 15, the delta contains:
    //   k1=2(op=0, dml_factor=1)  → kept
    //   k1=3(op=1, dml_factor=-1) → delete_sign=1 → deleted
    //   k1=4(op=0, dml_factor=1)  → kept
    //   k1=5(op=0, dml_factor=1)  → kept (new row)
    sql """INSERT INTO t_ivm_basic_filter_base VALUES (5, 50);"""

    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic_filter INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic_filter")

    // Only op=0 rows surviving the filter: k1=2, k1=4, k1=5 (k1=3 deleted, k1=1 filtered)
    order_qt_filter_after_incremental """SELECT k1, v1 FROM mv_ivm_basic_filter"""

    // Step 3: COMPLETE refresh sees the current filtered base-table snapshot again.
    // k1=3 still does not exist physically, so only k1=2,4,5 remain.
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic_filter COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic_filter")
    advance_ivm_stream_offset("mv_ivm_basic_filter")

    order_qt_filter_after_restore """SELECT k1, v1 FROM mv_ivm_basic_filter"""

    // Step 4: Upsert k1=3 to binlog_op=0, then INCREMENTAL — all filter-passing rows survive
    sql """INSERT INTO t_ivm_basic_filter_base VALUES (3, 30);"""

    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic_filter INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_basic_filter")

    order_qt_filter_after_upsert """SELECT k1, v1 FROM mv_ivm_basic_filter"""

}
