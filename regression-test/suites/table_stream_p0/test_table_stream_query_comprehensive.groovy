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

// Comprehensive regression for general table-stream queries. Modeled after
// test_binlog_changes_syntax but targeting the named STREAM object (with its
// persistent per-partition offset) instead of the timestamp-based @incr TVF.
//
// Dimensions covered:
//   - append_only vs min_delta consumption semantics
//   - show_initial_rows = true vs false
//   - MoW (unique + merge_on_write) vs DUP tables
//   - correctness before / after INSERT ... SELECT FROM stream advances offset
//   - partitioned (RANGE / LIST) vs non-partitioned tables
//
// Notes on assertions:
//   - __DORIS_STREAM_CHANGE_TYPE_COL__ is a stable string (APPEND / DELETE /
//     UPDATE_BEFORE / UPDATE_AFTER) and is asserted directly.
//   - __DORIS_STREAM_SEQUENCE_COL__ is a real commit tso (dynamic), so it is
//     only checked for "non-null / positive", never hard-coded.
//   - Pure SELECT FROM stream does NOT advance the offset; only
//     INSERT ... SELECT FROM stream does. After advancing, the stream returns
//     0 rows until new changes arrive.
suite("test_table_stream_query_comprehensive", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    sql "DROP DATABASE IF EXISTS test_table_stream_query_comprehensive_db"
    sql "CREATE DATABASE test_table_stream_query_comprehensive_db"
    sql "USE test_table_stream_query_comprehensive_db"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    // Stable accessor for nullable cells.
    def asStr = { row, idx -> row[idx] == null ? "null" : row[idx].toString() }
    // Order key so UPDATE_BEFORE/UPDATE_AFTER/DELETE/APPEND sort deterministically.
    def changeOrder = """
        CASE __DORIS_STREAM_CHANGE_TYPE_COL__
            WHEN 'UPDATE_BEFORE' THEN 0
            WHEN 'UPDATE_AFTER' THEN 1
            WHEN 'DELETE' THEN 2
            WHEN 'APPEND' THEN 3
            ELSE 9
        END
    """

    try {
        sql "DROP STREAM IF EXISTS dup_ao_stream"
        sql "DROP TABLE IF EXISTS dup_ao_base"
        sql "DROP TABLE IF EXISTS dup_ao_target"
        sql "DROP STREAM IF EXISTS dup_init_stream"
        sql "DROP TABLE IF EXISTS dup_init_base"
        sql "DROP TABLE IF EXISTS dup_init_target"
        sql "DROP STREAM IF EXISTS dup_md_stream"
        sql "DROP TABLE IF EXISTS dup_md_base"
        sql "DROP STREAM IF EXISTS mow_md_stream"
        sql "DROP TABLE IF EXISTS mow_md_base"
        sql "DROP TABLE IF EXISTS mow_md_target"
        sql "DROP STREAM IF EXISTS mow_init_stream"
        sql "DROP TABLE IF EXISTS mow_init_base"
        sql "DROP TABLE IF EXISTS mow_init_target"
        sql "DROP STREAM IF EXISTS mow_ao_stream"
        sql "DROP TABLE IF EXISTS mow_ao_base"
        sql "DROP STREAM IF EXISTS mow_range_stream"
        sql "DROP TABLE IF EXISTS mow_range_base"
        sql "DROP TABLE IF EXISTS mow_range_target"
        sql "DROP STREAM IF EXISTS dup_list_stream"
        sql "DROP TABLE IF EXISTS dup_list_base"
        sql "DROP TABLE IF EXISTS dup_list_target"

        // ============================================================
        // Section A. DUP, non-partitioned, append_only, initial=false.
        //   Focus: incremental APPEND rows and offset advancement across
        //   two consecutive INSERT ... SELECT FROM stream consumptions.
        //
        //   timeline:
        //     seed (before stream): (1,'a'),(2,'b')      -- not visible (initial=false)
        //     |-- CREATE STREAM --|
        //     batch1: (3,'c'),(4,'d')
        //     consume -> target gets 3,4 ; stream now empty
        //     batch2: (5,'e')
        //     consume -> target gets 3,4,5 ; stream now empty
        // ============================================================
        sql """
            CREATE TABLE dup_ao_base (
                id BIGINT,
                v VARCHAR(16)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "ROW"
            )
        """
        sql """
            CREATE TABLE dup_ao_target (
                id BIGINT,
                v VARCHAR(16)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO dup_ao_base VALUES (1, 'a'), (2, 'b')"
        sql """
            CREATE STREAM dup_ao_stream
            ON TABLE dup_ao_base
            PROPERTIES (
                "type" = "append_only",
                "show_initial_rows" = "false"
            )
        """
        sql "INSERT INTO dup_ao_base VALUES (3, 'c'), (4, 'd')"
        sql "sync"
        sleep(1200)

        // A.1 incremental rows only, no seed; all APPEND.
        def aBatch1 = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM dup_ao_stream
            ORDER BY id
        """
        assertEquals(2, aBatch1.size())
        assertEquals("3", asStr(aBatch1[0], 0))
        assertEquals("c", asStr(aBatch1[0], 1))
        assertEquals("APPEND", asStr(aBatch1[0], 2))
        assertEquals("4", asStr(aBatch1[1], 0))
        assertEquals("APPEND", asStr(aBatch1[1], 2))

        // A.2 pure SELECT does NOT advance: re-query returns the same rows.
        def aBatch1Again = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM dup_ao_stream
            ORDER BY id
        """
        assertEquals(aBatch1, aBatch1Again)

        // A.3 consume via INSERT ... SELECT advances offset; stream now empty.
        sql "INSERT INTO dup_ao_target SELECT id, v FROM dup_ao_stream"
        def aAfterConsume = sql "SELECT id, v FROM dup_ao_stream"
        assertEquals(0, aAfterConsume.size())

        // A.4 second batch only sees data after the advanced offset.
        sql "INSERT INTO dup_ao_base VALUES (5, 'e')"
        sql "sync"
        sleep(1200)
        def aBatch2 = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM dup_ao_stream
            ORDER BY id
        """
        assertEquals(1, aBatch2.size())
        assertEquals("5", asStr(aBatch2[0], 0))
        assertEquals("APPEND", asStr(aBatch2[0], 2))

        sql "INSERT INTO dup_ao_target SELECT id, v FROM dup_ao_stream"
        assertEquals(0, sql("SELECT id, v FROM dup_ao_stream").size())
        order_qt_dup_ao_target "SELECT id, v FROM dup_ao_target ORDER BY id"

        // ============================================================
        // Section B. DUP, non-partitioned, append_only, initial=true.
        //   Focus: historical seed rows are returned first (APPEND), then
        //   the stream switches to incremental after consumption.
        // ============================================================
        sql """
            CREATE TABLE dup_init_base (
                id BIGINT,
                v VARCHAR(16)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "ROW"
            )
        """
        sql """
            CREATE TABLE dup_init_target (
                id BIGINT,
                v VARCHAR(16)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO dup_init_base VALUES (1, 'a'), (2, 'b')"
        sql """
            CREATE STREAM dup_init_stream
            ON TABLE dup_init_base
            PROPERTIES (
                "type" = "append_only",
                "show_initial_rows" = "true"
            )
        """
        sql "sync"
        sleep(1200)

        // B.1 first query returns the historical seed rows as APPEND.
        def bHistory = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__, __DORIS_STREAM_SEQUENCE_COL__
            FROM dup_init_stream
            ORDER BY id
        """
        assertEquals(2, bHistory.size())
        assertEquals("1", asStr(bHistory[0], 0))
        assertEquals("APPEND", asStr(bHistory[0], 2))
        assertEquals("2", asStr(bHistory[1], 0))
        assertEquals("APPEND", asStr(bHistory[1], 2))
        assertTrue(bHistory.every { it[3] != null })

        // B.2 consume history; stream becomes empty.
        sql "INSERT INTO dup_init_target SELECT id, v FROM dup_init_stream"
        assertEquals(0, sql("SELECT id, v FROM dup_init_stream").size())

        // B.3 new write after history consumed shows up as incremental APPEND.
        sql "INSERT INTO dup_init_base VALUES (3, 'c')"
        sql "sync"
        sleep(1200)
        def bIncr = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM dup_init_stream
            ORDER BY id
        """
        assertEquals(1, bIncr.size())
        assertEquals("3", asStr(bIncr[0], 0))
        assertEquals("APPEND", asStr(bIncr[0], 2))

        sql "INSERT INTO dup_init_target SELECT id, v FROM dup_init_stream"
        order_qt_dup_init_target "SELECT id, v FROM dup_init_target ORDER BY id"

        // ============================================================
        // Section C. DUP, non-partitioned, type=min_delta requested but
        //   forcibly degraded to APPEND_ONLY for duplicate tables.
        //   Focus: a DUP table never folds; re-inserted same key and a
        //   DELETE statement (which produces no binlog op for DUP) leave
        //   only plain APPEND rows.
        // ============================================================
        sql """
            CREATE TABLE dup_md_base (
                id BIGINT,
                v INT
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "ROW"
            )
        """
        sql """
            CREATE STREAM dup_md_stream
            ON TABLE dup_md_base
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        sql "INSERT INTO dup_md_base VALUES (1, 10)"
        sql "INSERT INTO dup_md_base VALUES (1, 11)"
        sql "INSERT INTO dup_md_base VALUES (2, 20)"
        sql "sync"
        sleep(1200)

        // Every physical insert is preserved as APPEND; no folding, no
        // UPDATE_BEFORE/UPDATE_AFTER/DELETE even though type=min_delta.
        def cRows = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM dup_md_stream
            ORDER BY id, v
        """
        assertEquals(3, cRows.size())
        cRows.each { assertEquals("APPEND", asStr(it, 2)) }
        assertEquals("1", asStr(cRows[0], 0))
        assertEquals("10", asStr(cRows[0], 1))
        assertEquals("1", asStr(cRows[1], 0))
        assertEquals("11", asStr(cRows[1], 1))
        assertEquals("2", asStr(cRows[2], 0))
        assertEquals("20", asStr(cRows[2], 1))

        // ============================================================
        // Section D. MoW, non-partitioned, min_delta, initial=false.
        //   Focus: fold semantics + correctness of fold baseline AFTER the
        //   offset is advanced by a consumption.
        //
        //   seed (before stream): (1,10),(3,30)
        //   batch1: key1 11->12->13 (multi update), key2 insert20+delete (SKIP),
        //           key3 delete, key4 insert40
        //     expect: key1 UB(10)/UA(13), key3 DELETE(30), key4 APPEND(40) = 4 rows
        //   consume -> stream empty
        //   batch2: key4 update41, key5 insert50
        //     expect: key4 UB(40)/UA(41)  -- baseline 40 is the post-consume value,
        //             key5 APPEND(50)
        // ============================================================
        sql """
            CREATE TABLE mow_md_base (
                id BIGINT,
                v INT
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        sql """
            CREATE TABLE mow_md_target (
                id BIGINT,
                v INT,
                change_type VARCHAR(32)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO mow_md_base VALUES (1, 10), (3, 30)"
        sql """
            CREATE STREAM mow_md_stream
            ON TABLE mow_md_base
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        // batch1
        sql "INSERT INTO mow_md_base VALUES (1, 11)"
        sql "INSERT INTO mow_md_base VALUES (1, 12)"
        sql "INSERT INTO mow_md_base VALUES (1, 13)"
        sql "INSERT INTO mow_md_base VALUES (2, 20)"
        sql "DELETE FROM mow_md_base WHERE id = 2"
        sql "DELETE FROM mow_md_base WHERE id = 3"
        sql "INSERT INTO mow_md_base VALUES (4, 40)"
        sql "sync"
        sleep(1200)

        def dBatch1 = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM mow_md_stream
            ORDER BY id, ${changeOrder}, v
        """
        assertEquals(4, dBatch1.size())
        assertEquals("1", asStr(dBatch1[0], 0))
        assertEquals("10", asStr(dBatch1[0], 1))
        assertEquals("UPDATE_BEFORE", asStr(dBatch1[0], 2))
        assertEquals("1", asStr(dBatch1[1], 0))
        assertEquals("13", asStr(dBatch1[1], 1))
        assertEquals("UPDATE_AFTER", asStr(dBatch1[1], 2))
        assertEquals("3", asStr(dBatch1[2], 0))
        assertEquals("30", asStr(dBatch1[2], 1))
        assertEquals("DELETE", asStr(dBatch1[2], 2))
        assertEquals("4", asStr(dBatch1[3], 0))
        assertEquals("40", asStr(dBatch1[3], 1))
        assertEquals("APPEND", asStr(dBatch1[3], 2))

        // consume batch1
        sql """
            INSERT INTO mow_md_target
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__ FROM mow_md_stream
        """
        assertEquals(0, sql("SELECT id, v FROM mow_md_stream").size())

        // batch2: verify fold baseline is the post-consume value.
        sql "INSERT INTO mow_md_base VALUES (4, 41)"
        sql "INSERT INTO mow_md_base VALUES (5, 50)"
        sql "sync"
        sleep(1200)
        def dBatch2 = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM mow_md_stream
            ORDER BY id, ${changeOrder}, v
        """
        assertEquals(3, dBatch2.size())
        assertEquals("4", asStr(dBatch2[0], 0))
        assertEquals("40", asStr(dBatch2[0], 1))
        assertEquals("UPDATE_BEFORE", asStr(dBatch2[0], 2))
        assertEquals("4", asStr(dBatch2[1], 0))
        assertEquals("41", asStr(dBatch2[1], 1))
        assertEquals("UPDATE_AFTER", asStr(dBatch2[1], 2))
        assertEquals("5", asStr(dBatch2[2], 0))
        assertEquals("50", asStr(dBatch2[2], 1))
        assertEquals("APPEND", asStr(dBatch2[2], 2))

        sql """
            INSERT INTO mow_md_target
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__ FROM mow_md_stream
        """
        order_qt_mow_md_target "SELECT id, v, change_type FROM mow_md_target ORDER BY id, change_type, v"

        // ============================================================
        // Section E. MoW, non-partitioned, min_delta, initial=true.
        //   Focus: history rows returned first as APPEND; a key deleted in
        //   the incremental window does not appear in history; after history
        //   is consumed the stream folds subsequent incremental changes.
        //
        //   seed (before stream): (1,11),(2,11),(3,11)
        //   incr before first read: insert (4,11), delete id=2
        //   first read (history): 1,3,4 as APPEND (id=2 net-removed)
        //   consume -> target gets 1,3,4 ; stream empty
        //   incr: update id=1 -> 99
        //   read: UB(11)/UA(99)
        // ============================================================
        sql """
            CREATE TABLE mow_init_base (
                id BIGINT,
                v INT
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        sql """
            CREATE TABLE mow_init_target (
                id BIGINT,
                v INT
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO mow_init_base VALUES (1, 11), (2, 11), (3, 11)"
        sql """
            CREATE STREAM mow_init_stream
            ON TABLE mow_init_base
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "true"
            )
        """
        sql "INSERT INTO mow_init_base VALUES (4, 11)"
        sql "DELETE FROM mow_init_base WHERE id = 2"
        sql "sync"
        sleep(1200)

        def eHistory = sql """
            SELECT id, __DORIS_STREAM_CHANGE_TYPE_COL__, __DORIS_STREAM_SEQUENCE_COL__
            FROM mow_init_stream
            ORDER BY id
        """
        assertEquals(3, eHistory.size())
        assertEquals("1", asStr(eHistory[0], 0))
        assertEquals("APPEND", asStr(eHistory[0], 1))
        assertEquals("3", asStr(eHistory[1], 0))
        assertEquals("APPEND", asStr(eHistory[1], 1))
        assertEquals("4", asStr(eHistory[2], 0))
        assertEquals("APPEND", asStr(eHistory[2], 1))
        assertTrue(eHistory.every { it[2] != null })

        sql "INSERT INTO mow_init_target SELECT id, v FROM mow_init_stream"
        assertEquals(0, sql("SELECT id, v FROM mow_init_stream").size())

        // incremental fold after history is consumed.
        sql "INSERT INTO mow_init_base VALUES (1, 99)"
        sql "sync"
        sleep(1200)
        def eIncr = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM mow_init_stream
            ORDER BY id, ${changeOrder}, v
        """
        assertEquals(2, eIncr.size())
        assertEquals("1", asStr(eIncr[0], 0))
        assertEquals("11", asStr(eIncr[0], 1))
        assertEquals("UPDATE_BEFORE", asStr(eIncr[0], 2))
        assertEquals("1", asStr(eIncr[1], 0))
        assertEquals("99", asStr(eIncr[1], 1))
        assertEquals("UPDATE_AFTER", asStr(eIncr[1], 2))

        order_qt_mow_init_target "SELECT id, v FROM mow_init_target ORDER BY id"

        // ============================================================
        // Section F. MoW, non-partitioned, append_only, initial=false.
        //   Focus: append_only on a MoW table only keeps the first INSERT of
        //   a brand-new key; updates on existing keys are filtered out, and a
        //   later DELETE does not undo the APPEND.
        //
        //   seed (before stream): (1,10)
        //   incr: update id=1->11, insert id=2->20, insert id=3->30 then delete id=3
        //   expect APPEND of key2 and key3 only (key1 update filtered).
        // ============================================================
        sql """
            CREATE TABLE mow_ao_base (
                id BIGINT,
                v INT
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        sql "INSERT INTO mow_ao_base VALUES (1, 10)"
        sql """
            CREATE STREAM mow_ao_stream
            ON TABLE mow_ao_base
            PROPERTIES (
                "type" = "append_only",
                "show_initial_rows" = "false"
            )
        """
        sql "INSERT INTO mow_ao_base VALUES (1, 11)"
        sql "INSERT INTO mow_ao_base VALUES (2, 20)"
        sql "INSERT INTO mow_ao_base VALUES (3, 30)"
        sql "DELETE FROM mow_ao_base WHERE id = 3"
        sql "sync"
        sleep(1200)

        def fRows = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM mow_ao_stream
            ORDER BY id
        """
        assertEquals(2, fRows.size())
        def fIds = fRows.collect { asStr(it, 0) }.toSet()
        assertEquals(["2", "3"] as Set, fIds)
        fRows.each { assertEquals("APPEND", asStr(it, 2)) }

        // ============================================================
        // Section G. MoW, RANGE partitioned, min_delta, initial=false.
        //   Focus: per-partition offsets advance independently. Writing only
        //   one partition advances only that partition's offset; the other
        //   partition's offset is untouched.
        //
        //   partitions p1: id<10, p2: 10<=id<20
        //   seed: (1,10) in p1, (11,10) in p2
        //   batch1: update id=1->100 (p1), insert id=12->120 (p2)
        //     expect: p1 UB(10)/UA(100), p2 APPEND(120)
        //   consume both -> stream empty
        //   batch2: only p2 -> update id=12->121
        //     expect: only p2 UB(120)/UA(121); p1 stays empty.
        // ============================================================
        sql """
            CREATE TABLE mow_range_base (
                id BIGINT,
                v INT
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            PARTITION BY RANGE(id)
            (
                PARTITION p1 VALUES LESS THAN (10),
                PARTITION p2 VALUES LESS THAN (20)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        sql """
            CREATE TABLE mow_range_target (
                id BIGINT,
                v INT,
                change_type VARCHAR(32)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO mow_range_base VALUES (1, 10), (11, 10)"
        sql """
            CREATE STREAM mow_range_stream
            ON TABLE mow_range_base
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        // batch1 touches both partitions.
        sql "INSERT INTO mow_range_base VALUES (1, 100)"
        sql "INSERT INTO mow_range_base VALUES (12, 120)"
        sql "sync"
        sleep(1200)

        def gBatch1 = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM mow_range_stream
            ORDER BY id, ${changeOrder}, v
        """
        assertEquals(3, gBatch1.size())
        assertEquals("1", asStr(gBatch1[0], 0))
        assertEquals("10", asStr(gBatch1[0], 1))
        assertEquals("UPDATE_BEFORE", asStr(gBatch1[0], 2))
        assertEquals("1", asStr(gBatch1[1], 0))
        assertEquals("100", asStr(gBatch1[1], 1))
        assertEquals("UPDATE_AFTER", asStr(gBatch1[1], 2))
        assertEquals("12", asStr(gBatch1[2], 0))
        assertEquals("120", asStr(gBatch1[2], 1))
        assertEquals("APPEND", asStr(gBatch1[2], 2))

        sql """
            INSERT INTO mow_range_target
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__ FROM mow_range_stream
        """
        assertEquals(0, sql("SELECT id, v FROM mow_range_stream").size())

        // batch2 touches only p2; p1 offset must not regress / re-emit.
        sql "INSERT INTO mow_range_base VALUES (12, 121)"
        sql "sync"
        sleep(1200)
        def gBatch2 = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM mow_range_stream
            ORDER BY id, ${changeOrder}, v
        """
        assertEquals(2, gBatch2.size())
        assertEquals("12", asStr(gBatch2[0], 0))
        assertEquals("120", asStr(gBatch2[0], 1))
        assertEquals("UPDATE_BEFORE", asStr(gBatch2[0], 2))
        assertEquals("12", asStr(gBatch2[1], 0))
        assertEquals("121", asStr(gBatch2[1], 1))
        assertEquals("UPDATE_AFTER", asStr(gBatch2[1], 2))

        sql """
            INSERT INTO mow_range_target
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__ FROM mow_range_stream
        """
        order_qt_mow_range_target "SELECT id, v, change_type FROM mow_range_target ORDER BY id, change_type, v"

        // ============================================================
        // Section H. DUP, LIST partitioned, append_only, initial=true.
        //   Focus: historical rows across multiple partitions returned first;
        //   after consumption each partition switches to incremental
        //   independently.
        // ============================================================
        sql """
            CREATE TABLE dup_list_base (
                id INT,
                v VARCHAR(16)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            PARTITION BY LIST(id)
            (
                PARTITION p1 VALUES IN (1),
                PARTITION p2 VALUES IN (2),
                PARTITION p3 VALUES IN (3)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "ROW"
            )
        """
        sql """
            CREATE TABLE dup_list_target (
                id INT,
                v VARCHAR(16)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO dup_list_base VALUES (1, 'p1'), (2, 'p2'), (3, 'p3')"
        sql """
            CREATE STREAM dup_list_stream
            ON TABLE dup_list_base
            PROPERTIES (
                "type" = "append_only",
                "show_initial_rows" = "true"
            )
        """
        sql "sync"
        sleep(1200)

        // H.1 history across 3 partitions, all APPEND.
        def hHistory = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM dup_list_stream
            ORDER BY id
        """
        assertEquals(3, hHistory.size())
        assertEquals("1", asStr(hHistory[0], 0))
        assertEquals("APPEND", asStr(hHistory[0], 2))
        assertEquals("2", asStr(hHistory[1], 0))
        assertEquals("3", asStr(hHistory[2], 0))

        sql "INSERT INTO dup_list_target SELECT id, v FROM dup_list_stream"
        assertEquals(0, sql("SELECT id, v FROM dup_list_stream").size())

        // H.2 each partition advances independently to incremental mode.
        sql "INSERT INTO dup_list_base VALUES (2, 'p2b')"
        sql "INSERT INTO dup_list_base VALUES (3, 'p3b')"
        sql "sync"
        sleep(1200)
        def hIncr = sql """
            SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM dup_list_stream
            ORDER BY id
        """
        assertEquals(2, hIncr.size())
        assertEquals("2", asStr(hIncr[0], 0))
        assertEquals("p2b", asStr(hIncr[0], 1))
        assertEquals("APPEND", asStr(hIncr[0], 2))
        assertEquals("3", asStr(hIncr[1], 0))
        assertEquals("p3b", asStr(hIncr[1], 1))
        assertEquals("APPEND", asStr(hIncr[1], 2))

        sql "INSERT INTO dup_list_target SELECT id, v FROM dup_list_stream"
        order_qt_dup_list_target "SELECT id, v FROM dup_list_target ORDER BY id, v"
    } finally {
        sql "DROP DATABASE IF EXISTS test_table_stream_query_comprehensive_db"
    }
}
