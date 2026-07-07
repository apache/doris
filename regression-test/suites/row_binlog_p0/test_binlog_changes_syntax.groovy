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

suite("test_binlog_changes_syntax", "nonConcurrent") {
    sql "DROP DATABASE IF EXISTS test_binlog_changes_syntax_db"
    sql "CREATE DATABASE test_binlog_changes_syntax_db"
    sql "USE test_binlog_changes_syntax_db"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    def dupTable = "changes_dup"
    def mowTable = "changes_mow"
    def mowSeqTable = "changes_mow_seq"
    def mowPartialTable = "changes_mow_partial"
    def mowBitmapTable = "changes_mow_bitmap"
    def incrTimeFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    try {
        sql "DROP TABLE IF EXISTS ${dupTable}"
        sql "DROP TABLE IF EXISTS ${mowTable}"
        sql "DROP TABLE IF EXISTS ${mowSeqTable}"
        sql "DROP TABLE IF EXISTS ${mowPartialTable}"
        sql "DROP TABLE IF EXISTS ${mowBitmapTable}"

        // ============================================================
        // 1. DUPLICATE table — multi-column + nullable + multi-rowset writes.
        //    binlog of dup table only records INSERT rows (op=0). With more
        //    columns and a re-inserted same-key, we ensure the changes()
        //    syntax preserves every physical row instead of de-duplicating.
        //
        //    timeline (each INSERT below produces an independent rowset):
        //      seed:  (1,10,'a',NULL)
        //             (2,20,'b','x')
        //      |---- dupT0 ----|
        //      win:   (3,30,'c','y')          rowset r1
        //             (4,40,NULL,'z')         rowset r2
        //             (3,31,'c2',NULL)        rowset r3 (same key=3)
        //      |---- dupT1 ----|
        //      late:  (5,50,'d','w')          rowset r4 (after dupT1)
        //
        //    Total binlog rows: 2 seed + 3 in-window + 1 late = 6.
        // ============================================================
        sql """
            CREATE TABLE ${dupTable} (
                id BIGINT,
                v1 INT,
                v2 VARCHAR(16),
                v3 VARCHAR(16) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "ROW"
            )
        """
        sql "INSERT INTO ${dupTable} VALUES (1, 10, 'a', NULL)"
        sql "INSERT INTO ${dupTable} VALUES (2, 20, 'b', 'x')"
        sql "sync"
        sleep(1200)
        def dupT0 = incrTimeFormat.format(new Date())
        sleep(1200)
        sql "INSERT INTO ${dupTable} VALUES (3, 30, 'c', 'y')"
        sql "INSERT INTO ${dupTable} VALUES (4, 40, NULL, 'z')"
        sql "INSERT INTO ${dupTable} VALUES (3, 31, 'c2', NULL)"
        sql "sync"
        sleep(1200)
        def dupT1 = incrTimeFormat.format(new Date())
        sleep(1200)
        sql "INSERT INTO ${dupTable} VALUES (5, 50, 'd', 'w')"
        sql "sync"

        // 1.1 APPEND_ONLY [dupT0, dupT1]: 3 in-window inserts including the
        //     re-inserted key=3.
        order_qt_dup_append_range """
            SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '${dupT0}',
                "endTimestamp" = "${dupT1}",
                "incrementType" = "APPEND_ONLY")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 1.2 DETAIL [dupT0, dupT1] must be identical to APPEND_ONLY for dup.
        order_qt_dup_detail_range """
            SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '${dupT0}',
                "endTimestamp" = "${dupT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 1.3 MIN_DELTA on dup table should be equivalent to APPEND_ONLY:
        //     dup table has no DELETE / UPDATE in binlog, so per-key folding
        //     degenerates to "all inserts kept as APPEND".
        order_qt_dup_min_delta_range """
            SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '${dupT0}',
                "endTimestamp" = "${dupT1}",
                "incrementType" = "MIN_DELTA")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 1.4 startTimestamp only: covers in-window + late insert.
        order_qt_dup_detail_start """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '${dupT0}',
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 1.5 endTimestamp only: covers seed + in-window inserts.
        order_qt_dup_detail_end """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr("endTimestamp" = "${dupT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 1.6 No timestamps: equals @incr() and equals DETAIL of full binlog.
        order_qt_dup_detail_all """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr("incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """
        order_qt_dup_incr_empty """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr()
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 1.7 Degenerate windows.
        //  - start > end: should yield empty.
        order_qt_dup_reversed """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '${dupT1}',
                "endTimestamp" = "${dupT0}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        //  - far-future start: empty.
        order_qt_dup_far_future """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '2999-01-01 00:00:00',
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        //  - far-past start + far-future end: equivalent to full binlog.
        order_qt_dup_full_cover """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '1971-01-01 00:00:00',
                "endTimestamp" = "2999-01-01 00:00:00",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 1.8 Cross-check against binlog() TVF — DETAIL with full window must
        //     return the same rowset as the underlying binlog TVF for a dup
        //     table (since both expose only INSERT rows).
        order_qt_dup_binlog_tvf """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM binlog("table" = "${dupTable}")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // ============================================================
        // 2. MoW table (UNIQUE KEY + enable_unique_key_merge_on_write
        //               + binlog.need_historical_value=true).
        //
        //    Workload weaves five distinct key behaviours inside [mowT0, mowT1]:
        //      key=1 (seed=10): 11 -> 12 -> 13   (3 updates across rowsets)
        //      key=2          : INSERT 20 -> DELETE -> INSERT 21
        //                       (resurrect)
        //      key=3 (seed=30): DELETE
        //      key=4          : INSERT 40
        //      key=5          : INSERT 50 -> DELETE
        //                       (insert-then-delete in same window)
        //    Outside-window:
        //      seed (before mowT0)  : INSERT 10 / 30
        //      late (after mowT1)   : INSERT 60
        //
        //    Expected per-key MIN_DELTA inside [mowT0, mowT1]:
        //      key=1  -> UPDATE_BEFORE(10) + UPDATE_AFTER(13)
        //      key=2  -> APPEND(21)
        //               (key=2 did not exist before mowT0, so the start-vs-end
        //                snapshot diff is a brand-new key.)
        //      key=3  -> DELETE(30)
        //      key=4  -> APPEND(40)
        //      key=5  -> SKIP (insert+delete inside the window)
        //    Total MIN_DELTA rows = 5.
        // ============================================================
        sql """
            CREATE TABLE ${mowTable} (
                id BIGINT,
                v1 INT,
                v2 VARCHAR(16) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        sql "INSERT INTO ${mowTable} VALUES (1, 10, 'a')"
        sql "INSERT INTO ${mowTable} VALUES (3, 30, 'c')"
        sql "sync"
        sleep(1200)
        def mowT0 = incrTimeFormat.format(new Date())
        sleep(1200)

        // key=1: three update rounds spread across rowsets.
        sql "INSERT INTO ${mowTable} VALUES (1, 11, 'a1')"
        sql "INSERT INTO ${mowTable} VALUES (1, 12, 'a2')"
        sql "INSERT INTO ${mowTable} VALUES (1, 13, 'a3')"
        // key=2: insert -> delete -> resurrect.
        sql "INSERT INTO ${mowTable} VALUES (2, 20, 'b')"
        sql "DELETE FROM ${mowTable} WHERE id = 2"
        sql "INSERT INTO ${mowTable} VALUES (2, 21, 'b1')"
        // key=3: delete only.
        sql "DELETE FROM ${mowTable} WHERE id = 3"
        // key=4: pure insert.
        sql "INSERT INTO ${mowTable} VALUES (4, 40, 'd')"
        // key=5: insert and then delete within the same window.
        sql "INSERT INTO ${mowTable} VALUES (5, 50, 'e')"
        sql "DELETE FROM ${mowTable} WHERE id = 5"
        sql "sync"

        sleep(1200)
        def mowT1 = incrTimeFormat.format(new Date())
        sleep(1200)
        sql "INSERT INTO ${mowTable} VALUES (6, 60, 'f')"
        sql "sync"

        // 2.1 APPEND_ONLY [mowT0, mowT1]: only brand-new keys with their first
        //     INSERT. Updates / deletes / resurrections are filtered out.
        //     Expected:
        //       key=2 first INSERT(20) is APPEND (key did not exist before).
        //       key=4 INSERT(40) is APPEND.
        //       key=5 INSERT(50) is APPEND (the later DELETE does not undo
        //                                   APPEND in APPEND_ONLY semantics).
        order_qt_mow_append_range """
            SELECT id, v1, v2, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr('startTimestamp' = '${mowT0}',
                "endTimestamp" = "${mowT1}",
                "incrementType" = "APPEND_ONLY")
            ORDER BY id, __DORIS_BINLOG_LSN__
        """

        // 2.2 MIN_DELTA [mowT0, mowT1]: per-key folded result.
        order_qt_mow_min_delta_range """
            SELECT id, v1, v2, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr('startTimestamp' = '${mowT0}',
                "endTimestamp" = "${mowT1}",
                "incrementType" = "MIN_DELTA")
            ORDER BY id, __DORIS_BINLOG_OP__, v1
        """
        // MIN_DELTA op codes (from __DORIS_BINLOG_OP__):
        //   0 = INSERT/APPEND, 1 = DELETE, 2 = UPDATE_BEFORE, 3 = UPDATE_AFTER
        // MIN_DELTA folds the start vs. end snapshot of the window, regardless
        // of intermediate insert/delete/insert sequences.
        // Expected 5 rows:
        //   (1, 10, *, 2)   + (1, 13, 'a3', 3)
        //   (2, 21, 'b1', 0)            -- key=2 did not exist before mowT0
        //   (3, 30, 'c', 1)
        //   (4, 40, 'd', 0)
        // 2.3 DETAIL [mowT0, mowT1]: every raw binlog row in the window.
        //     Each "INSERT into UNIQUE KEY MoW that hits an existing key" emits
        //     a UPDATE_BEFORE/UPDATE_AFTER pair instead of a plain INSERT. After
        //     a DELETE the key version still exists in row binlog, so a later
        //     INSERT on the same key is also recorded as an UPDATE (BEFORE is
        //     populated from the deleted snapshot, hence NULL value columns).
        //     Count breakdown:
        //       key=1: 3 updates -> 3 * 2 = 6 rows
        //       key=2: INSERT(20) + DELETE(20) + UPDATE(NULL -> 21) = 4 rows
        //       key=3: DELETE(30) = 1 row
        //       key=4: INSERT(40) = 1 row
        //       key=5: INSERT(50) + DELETE(50) = 2 rows
        //     Total = 6 + 4 + 1 + 1 + 2 = 14.
        order_qt_mow_detail_range """
            SELECT id, v1, v2, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr('startTimestamp' = '${mowT0}',
                "endTimestamp" = "${mowT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 2.4 startTimestamp only: includes the late (6,60).
        order_qt_mow_detail_start """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr('startTimestamp' = '${mowT0}',
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 2.5 endTimestamp only: includes the seed (1,10) and (3,30).
        order_qt_mow_detail_end """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr("endTimestamp" = "${mowT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """
        order_qt_mow_detail_all """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr("incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 2.6 MIN_DELTA across full binlog: per-key folding from binlog start
        //     (every key starts non-existent) to the final visible state.
        //     Even the "seed" inserts before mowT0 are inside the binlog
        //     timeline, so insert+delete pairs whose net effect is "no-op"
        //     collapse to SKIP rather than DELETE.
        //   key=1: APPEND(13)   (started non-existent -> ends at 13)
        //   key=2: APPEND(21)   (started non-existent -> ends at 21)
        //   key=3: SKIP         (insert 30 then delete -> net no-op)
        //   key=4: APPEND(40)
        //   key=5: SKIP
        //   key=6: APPEND(60)
        order_qt_mow_min_delta_all """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr("incrementType" = "MIN_DELTA")
            ORDER BY id, __DORIS_BINLOG_OP__, v1
        """

        // 2.7 Empty timestamps: min-delta binlog and equivalence with @incr().
        order_qt_mow_incr_empty """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr()
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 2.8 Degenerate window: start > end -> empty.
        order_qt_mow_reversed """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr('startTimestamp' = '${mowT1}',
                "endTimestamp" = "${mowT0}",
                "incrementType" = "MIN_DELTA")
        """

        // 2.9 Far-past + far-future window covers everything.
        order_qt_mow_full_cover """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr('startTimestamp' = '1971-01-01 00:00:00',
                "endTimestamp" = "2999-01-01 00:00:00",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // ============================================================
        // 3. MoW table with sequence column.
        //    Verify that out-of-order sequence writes are physically rejected
        //    and therefore do not land in binlog at all (DETAIL / MIN_DELTA /
        //    APPEND_ONLY all only see the accepted writes).
        // ============================================================
        sql """
            CREATE TABLE ${mowSeqTable} (
                id BIGINT,
                v1 INT,
                v2 VARCHAR(16)
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        sql """ALTER TABLE ${mowSeqTable}
                 ENABLE FEATURE "SEQUENCE_LOAD"
                 WITH PROPERTIES ("function_column.sequence_type" = "int")"""
        sql """
            INSERT INTO ${mowSeqTable}(id, v1, v2, __DORIS_SEQUENCE_COL__)
            VALUES (1, 100, 'p', 5)
        """
        sql "sync"
        sleep(1200)
        def seqT0 = incrTimeFormat.format(new Date())
        sleep(1200)
        // Out-of-order: smaller seq is physically rejected and is NOT recorded
        // by binlog. Final visible value should be (300, seq=10).
        sql """
            INSERT INTO ${mowSeqTable}(id, v1, v2, __DORIS_SEQUENCE_COL__)
            VALUES (1, 200, 'q', 3)
        """
        sql """
            INSERT INTO ${mowSeqTable}(id, v1, v2, __DORIS_SEQUENCE_COL__)
            VALUES (1, 300, 'r', 10)
        """
        sql "sync"
        sleep(1200)
        def seqT1 = incrTimeFormat.format(new Date())
        sleep(1200)

        // 3.1 DETAIL only captures physically-applied writes. The out-of-order
        //     write (seq=3) is rejected and never lands in binlog, so only the
        //     accepted update (seq=10) shows up.
        order_qt_seq_detail """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowSeqTable}@incr('startTimestamp' = '${seqT0}',
                "endTimestamp" = "${seqT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 3.2 MIN_DELTA collapses to BEFORE(100) -> AFTER(latest visible value).
        order_qt_seq_min_delta """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowSeqTable}@incr('startTimestamp' = '${seqT0}',
                "endTimestamp" = "${seqT1}",
                "incrementType" = "MIN_DELTA")
            ORDER BY __DORIS_BINLOG_OP__
        """
        order_qt_seq_visible """
            SELECT v1 FROM ${mowSeqTable} WHERE id = 1
        """

        // ============================================================
        // 4. MoW table with partial column update. Each partial update
        //    produces an UPDATE_BEFORE/UPDATE_AFTER pair in DETAIL, while
        //    MIN_DELTA must keep only first-BEFORE / last-AFTER.
        // ============================================================
        sql """
            CREATE TABLE ${mowPartialTable} (
                id BIGINT,
                v1 INT,
                v2 VARCHAR(16),
                v3 INT
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        sql "INSERT INTO ${mowPartialTable} VALUES (1, 10, 'a', 1000)"
        sql "sync"
        sleep(1200)
        def partT0 = incrTimeFormat.format(new Date())
        sleep(1200)
        sql "SET enable_unique_key_partial_update = true"
        sql "INSERT INTO ${mowPartialTable}(id, v1) VALUES (1, 11)"
        sql "INSERT INTO ${mowPartialTable}(id, v2) VALUES (1, 'b')"
        sql "INSERT INTO ${mowPartialTable}(id, v3) VALUES (1, 2000)"
        sql "SET enable_unique_key_partial_update = false"
        sql "sync"
        sleep(1200)
        def partT1 = incrTimeFormat.format(new Date())

        // 4.1 DETAIL: 3 partial updates -> 6 raw binlog rows.
        order_qt_part_detail """
            SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
            FROM ${mowPartialTable}@incr('startTimestamp' = '${partT0}',
                "endTimestamp" = "${partT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // 4.2 MIN_DELTA: collapses to BEFORE/AFTER pair for key=1, where AFTER
        //     reflects the merged final visible row.
        order_qt_part_min_delta """
            SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
            FROM ${mowPartialTable}@incr('startTimestamp' = '${partT0}',
                "endTimestamp" = "${partT1}",
                "incrementType" = "MIN_DELTA")
            ORDER BY __DORIS_BINLOG_OP__
        """
        order_qt_part_visible """
            SELECT v1, v2, v3 FROM ${mowPartialTable} WHERE id = 1
        """

        // 4.3 APPEND_ONLY in the same window: pure partial updates on an
        //     existing key produce no APPEND rows.
        order_qt_part_append """
            SELECT id, __DORIS_BINLOG_OP__
            FROM ${mowPartialTable}@incr('startTimestamp' = '${partT0}',
                "endTimestamp" = "${partT1}",
                "incrementType" = "APPEND_ONLY")
        """

        // ============================================================
        // 5. MoW table with BITMAP NOT NULL.
        //    This specifically exercises MIN_DELTA UPDATE_BEFORE/AFTER on a
        //    complex-type value column. Before the Column.java fix, the
        //    UPDATE_BEFORE row read from nullable __BEFORE__b__ would be
        //    copied into the non-nullable after-slot and BE would crash with
        //    "Bad cast from type ColumnNullable to ColumnComplexType<BITMAP>".
        // ============================================================
        sql """
            CREATE TABLE ${mowBitmapTable} (
                id BIGINT,
                b BITMAP NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        sql "INSERT INTO ${mowBitmapTable} VALUES (1, BITMAP_FROM_STRING('1,2'))"
        sql "sync"
        sleep(1200)
        def bitmapT0 = incrTimeFormat.format(new Date())
        sleep(1200)
        sql "INSERT INTO ${mowBitmapTable} VALUES (1, BITMAP_FROM_STRING('3,4'))"
        sql "sync"
        sleep(1200)
        def bitmapT1 = incrTimeFormat.format(new Date())

        order_qt_bitmap_min_delta """
            SELECT id, BITMAP_TO_STRING(b), __DORIS_BINLOG_OP__
            FROM ${mowBitmapTable}@incr('startTimestamp' = '${bitmapT0}',
                "endTimestamp" = "${bitmapT1}",
                "incrementType" = "MIN_DELTA")
            ORDER BY __DORIS_BINLOG_OP__
        """
    } finally {
        sql "DROP DATABASE IF EXISTS test_binlog_changes_syntax_db"
    }
}
