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

suite("test_binlog_changes_syntax") {
    if (isCloudMode()) {
        return
    }
    sql "DROP DATABASE IF EXISTS test_binlog_changes_syntax_db"
    sql "CREATE DATABASE test_binlog_changes_syntax_db"
    sql "USE test_binlog_changes_syntax_db"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    def dupTable = "changes_dup"
    def mowTable = "changes_mow"
    def mowSeqTable = "changes_mow_seq"
    def mowPartialTable = "changes_mow_partial"
    def incrTimeFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    // Helper: stable LSN sort accessor.
    def asStr = { row, idx -> row[idx] == null ? "null" : row[idx].toString() }

    try {
        sql "DROP TABLE IF EXISTS ${dupTable}"
        sql "DROP TABLE IF EXISTS ${mowTable}"
        sql "DROP TABLE IF EXISTS ${mowSeqTable}"
        sql "DROP TABLE IF EXISTS ${mowPartialTable}"

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
        def dupAppendRange = sql """
            SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '${dupT0}',
                "endTimestamp" = "${dupT1}",
                "incrementType" = "APPEND_ONLY")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(3, dupAppendRange.size())
        assertEquals("3", asStr(dupAppendRange[0], 0))
        assertEquals("30", asStr(dupAppendRange[0], 1))
        assertEquals("c", asStr(dupAppendRange[0], 2))
        assertEquals("y", asStr(dupAppendRange[0], 3))
        assertEquals("0", asStr(dupAppendRange[0], 4))
        assertEquals("4", asStr(dupAppendRange[1], 0))
        assertEquals("null", asStr(dupAppendRange[1], 2))
        assertEquals("z", asStr(dupAppendRange[1], 3))
        assertEquals("3", asStr(dupAppendRange[2], 0))
        assertEquals("31", asStr(dupAppendRange[2], 1))
        assertEquals("c2", asStr(dupAppendRange[2], 2))
        assertEquals("null", asStr(dupAppendRange[2], 3))

        // 1.2 DETAIL [dupT0, dupT1] must be identical to APPEND_ONLY for dup.
        def dupDetailRange = sql """
            SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '${dupT0}',
                "endTimestamp" = "${dupT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(dupAppendRange, dupDetailRange)

        // 1.3 MIN_DELTA on dup table should be equivalent to APPEND_ONLY:
        //     dup table has no DELETE / UPDATE in binlog, so per-key folding
        //     degenerates to "all inserts kept as APPEND".
        def dupMinDeltaRange = sql """
            SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '${dupT0}',
                "endTimestamp" = "${dupT1}",
                "incrementType" = "MIN_DELTA")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(dupAppendRange, dupMinDeltaRange)

        // 1.4 startTimestamp only: covers in-window + late insert.
        def dupDetailStart = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '${dupT0}',
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(4, dupDetailStart.size())
        assertEquals("5", asStr(dupDetailStart[3], 0))
        assertEquals("50", asStr(dupDetailStart[3], 1))

        // 1.5 endTimestamp only: covers seed + in-window inserts.
        def dupDetailEnd = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr("endTimestamp" = "${dupT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(5, dupDetailEnd.size())
        assertEquals("1", asStr(dupDetailEnd[0], 0))
        assertEquals("2", asStr(dupDetailEnd[1], 0))

        // 1.6 No timestamps: equals @incr() and equals DETAIL of full binlog.
        def dupDetailAll = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr("incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        def dupIncrEmpty = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr()
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(6, dupDetailAll.size())
        assertEquals(dupDetailAll, dupIncrEmpty)

        // 1.7 Degenerate windows.
        //  - start > end: should yield empty.
        def dupReversed = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '${dupT1}',
                "endTimestamp" = "${dupT0}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(0, dupReversed.size())

        //  - far-future start: empty.
        def dupFarFuture = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '2999-01-01 00:00:00',
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(0, dupFarFuture.size())

        //  - far-past start + far-future end: equivalent to full binlog.
        def dupFullCover = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${dupTable}@incr('startTimestamp' = '1971-01-01 00:00:00',
                "endTimestamp" = "2999-01-01 00:00:00",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(dupDetailAll, dupFullCover)

        // 1.8 Cross-check against binlog() TVF — DETAIL with full window must
        //     return the same rowset as the underlying binlog TVF for a dup
        //     table (since both expose only INSERT rows).
        def dupBinlogTvf = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM binlog("table" = "${dupTable}")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(dupBinlogTvf.size(), dupDetailAll.size())
        for (int i = 0; i < dupBinlogTvf.size(); i++) {
            assertEquals(asStr(dupBinlogTvf[i], 0), asStr(dupDetailAll[i], 0))
            assertEquals(asStr(dupBinlogTvf[i], 1), asStr(dupDetailAll[i], 1))
        }

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
        def mowAppendRange = sql """
            SELECT id, v1, v2, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr('startTimestamp' = '${mowT0}',
                "endTimestamp" = "${mowT1}",
                "incrementType" = "APPEND_ONLY")
            ORDER BY id, __DORIS_BINLOG_LSN__
        """
        assertEquals(3, mowAppendRange.size())
        // Sort by id is enough to locate them.
        def appendIds = mowAppendRange.collect { asStr(it, 0) }.toSet()
        assertEquals(["2", "4", "5"] as Set, appendIds)
        mowAppendRange.each { assertEquals("0", asStr(it, 3)) }

        // 2.2 MIN_DELTA [mowT0, mowT1]: per-key folded result.
        def mowMinDeltaRange = sql """
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
        assertEquals(5, mowMinDeltaRange.size())
        def mdByKey = [:]
        mowMinDeltaRange.each {
            def k = asStr(it, 0)
            mdByKey.computeIfAbsent(k, { _ -> [] }).add(it)
        }
        // key=1
        assertEquals(2, mdByKey["1"].size())
        def k1Ops = mdByKey["1"].collect { asStr(it, 3) }.toSet()
        assertEquals(["2", "3"] as Set, k1Ops)
        def k1Before = mdByKey["1"].find { asStr(it, 3) == "2" }
        def k1After = mdByKey["1"].find { asStr(it, 3) == "3" }
        assertEquals("10", asStr(k1Before, 1))
        assertEquals("13", asStr(k1After, 1))
        assertEquals("a3", asStr(k1After, 2))
        // key=2: did not exist before mowT0; insert+delete+insert collapses to
        //        a net APPEND(21).
        assertEquals(1, mdByKey["2"].size())
        assertEquals("0", asStr(mdByKey["2"][0], 3))
        assertEquals("21", asStr(mdByKey["2"][0], 1))
        assertEquals("b1", asStr(mdByKey["2"][0], 2))
        // key=3: pure DELETE
        assertEquals(1, mdByKey["3"].size())
        assertEquals("1", asStr(mdByKey["3"][0], 3))
        assertEquals("30", asStr(mdByKey["3"][0], 1))
        // key=4: pure APPEND
        assertEquals(1, mdByKey["4"].size())
        assertEquals("0", asStr(mdByKey["4"][0], 3))
        assertEquals("40", asStr(mdByKey["4"][0], 1))
        // key=5: insert-then-delete inside window collapses to nothing.
        assertEquals(null, mdByKey["5"])

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
        def mowDetailRange = sql """
            SELECT id, v1, v2, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr('startTimestamp' = '${mowT0}',
                "endTimestamp" = "${mowT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(14, mowDetailRange.size())
        // Detail must contain every concrete value we ever wrote on key=1.
        def k1Vals = mowDetailRange.findAll { asStr(it, 0) == "1" }.collect { asStr(it, 1) }.toSet()
        assertTrue(k1Vals.containsAll(["10", "11", "12", "13"] as Set))

        // 2.4 startTimestamp only: includes the late (6,60).
        def mowDetailStart = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr('startTimestamp' = '${mowT0}',
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(15, mowDetailStart.size())
        assertEquals("6", asStr(mowDetailStart[14], 0))
        assertEquals("60", asStr(mowDetailStart[14], 1))
        assertEquals("0", asStr(mowDetailStart[14], 2))

        // 2.5 endTimestamp only: includes the seed (1,10) and (3,30).
        def mowDetailEnd = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr("endTimestamp" = "${mowT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(16, mowDetailEnd.size())
        assertEquals("1", asStr(mowDetailEnd[0], 0))
        assertEquals("10", asStr(mowDetailEnd[0], 1))
        assertEquals("0", asStr(mowDetailEnd[0], 2))
        assertEquals("3", asStr(mowDetailEnd[1], 0))
        assertEquals("30", asStr(mowDetailEnd[1], 1))
        assertEquals("0", asStr(mowDetailEnd[1], 2))

        // 2.6 Empty timestamps: full binlog and equivalence with @incr().
        def mowDetailAll = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr("incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        def mowIncrEmpty = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr()
            ORDER BY __DORIS_BINLOG_LSN__
        """
        // 2 seed + 14 in-window + 1 late = 17.
        assertEquals(17, mowDetailAll.size())
        assertEquals(mowDetailAll, mowIncrEmpty)

        // 2.7 MIN_DELTA across full binlog: per-key folding from binlog start
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
        def mowMinDeltaAll = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr("incrementType" = "MIN_DELTA")
            ORDER BY id, __DORIS_BINLOG_OP__, v1
        """
        assertEquals(4, mowMinDeltaAll.size())
        def mdAllByKey = [:]
        mowMinDeltaAll.each {
            mdAllByKey.computeIfAbsent(asStr(it, 0), { _ -> [] }).add(it)
        }
        assertEquals(1, mdAllByKey["1"].size())
        assertEquals("0", asStr(mdAllByKey["1"][0], 2))
        assertEquals("13", asStr(mdAllByKey["1"][0], 1))
        // key=2 over the full timeline: the key did not exist before mowT0,
        // so the net effect is APPEND(21). MIN_DELTA op = 0.
        assertEquals(1, mdAllByKey["2"].size())
        assertEquals("0", asStr(mdAllByKey["2"][0], 2))
        assertEquals("21", asStr(mdAllByKey["2"][0], 1))
        // key=3: full-timeline insert(30) + delete folds to SKIP.
        assertEquals(null, mdAllByKey["3"])
        assertEquals("0", asStr(mdAllByKey["4"][0], 2))
        assertEquals(null, mdAllByKey["5"])
        assertEquals("0", asStr(mdAllByKey["6"][0], 2))
        assertEquals("60", asStr(mdAllByKey["6"][0], 1))

        // 2.8 Degenerate window: start > end -> empty.
        def mowReversed = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr('startTimestamp' = '${mowT1}',
                "endTimestamp" = "${mowT0}",
                "incrementType" = "MIN_DELTA")
        """
        assertEquals(0, mowReversed.size())

        // 2.9 Far-past + far-future window covers everything.
        def mowFullCover = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowTable}@incr('startTimestamp' = '1971-01-01 00:00:00',
                "endTimestamp" = "2999-01-01 00:00:00",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(mowDetailAll, mowFullCover)

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
        def seqDetail = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowSeqTable}@incr('startTimestamp' = '${seqT0}',
                "endTimestamp" = "${seqT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        // Only the accepted update produces 1 BEFORE/AFTER pair = 2 rows.
        assertEquals(2, seqDetail.size())

        // 3.2 MIN_DELTA collapses to BEFORE(100) -> AFTER(latest visible value).
        def seqMinDelta = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${mowSeqTable}@incr('startTimestamp' = '${seqT0}',
                "endTimestamp" = "${seqT1}",
                "incrementType" = "MIN_DELTA")
            ORDER BY __DORIS_BINLOG_OP__
        """
        assertEquals(2, seqMinDelta.size())
        // Op 2 is UPDATE_BEFORE, op 3 is UPDATE_AFTER.
        assertEquals("2", asStr(seqMinDelta[0], 2))
        assertEquals("100", asStr(seqMinDelta[0], 1))
        assertEquals("3", asStr(seqMinDelta[1], 2))
        // The visible AFTER value must be the latest visible row. Read it back
        // from the table to avoid hard-coding implementation details.
        def visible = sql "SELECT v1 FROM ${mowSeqTable} WHERE id = 1"
        assertEquals(asStr(visible[0], 0), asStr(seqMinDelta[1], 1))

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
        def partDetail = sql """
            SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
            FROM ${mowPartialTable}@incr('startTimestamp' = '${partT0}',
                "endTimestamp" = "${partT1}",
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(6, partDetail.size())

        // 4.2 MIN_DELTA: collapses to BEFORE/AFTER pair for key=1, where AFTER
        //     reflects the merged final visible row.
        def partMinDelta = sql """
            SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
            FROM ${mowPartialTable}@incr('startTimestamp' = '${partT0}',
                "endTimestamp" = "${partT1}",
                "incrementType" = "MIN_DELTA")
            ORDER BY __DORIS_BINLOG_OP__
        """
        assertEquals(2, partMinDelta.size())
        assertEquals("2", asStr(partMinDelta[0], 4))
        assertEquals("10", asStr(partMinDelta[0], 1))
        assertEquals("a", asStr(partMinDelta[0], 2))
        assertEquals("1000", asStr(partMinDelta[0], 3))
        assertEquals("3", asStr(partMinDelta[1], 4))
        def partVisible = sql "SELECT v1, v2, v3 FROM ${mowPartialTable} WHERE id = 1"
        assertEquals(asStr(partVisible[0], 0), asStr(partMinDelta[1], 1))
        assertEquals(asStr(partVisible[0], 1), asStr(partMinDelta[1], 2))
        assertEquals(asStr(partVisible[0], 2), asStr(partMinDelta[1], 3))

        // 4.3 APPEND_ONLY in the same window: pure partial updates on an
        //     existing key produce no APPEND rows.
        def partAppend = sql """
            SELECT id, __DORIS_BINLOG_OP__
            FROM ${mowPartialTable}@incr('startTimestamp' = '${partT0}',
                "endTimestamp" = "${partT1}",
                "incrementType" = "APPEND_ONLY")
        """
        assertEquals(0, partAppend.size())
    } finally {
        sql "DROP DATABASE IF EXISTS test_binlog_changes_syntax_db"
    }
}
