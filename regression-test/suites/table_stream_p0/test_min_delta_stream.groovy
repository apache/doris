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

suite("test_min_delta_stream", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    sql "DROP DATABASE IF EXISTS test_min_delta_stream_db"
    sql "CREATE DATABASE test_min_delta_stream_db"
    sql "USE test_min_delta_stream_db"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    def ukBase = "md_uk_base"
    def ukStream = "md_uk_stream"
    def ukSkipBase = "md_uk_skip_base"
    def ukSkipStream = "md_uk_skip_stream"
    def ukMultiBase = "md_uk_multi_base"
    def ukMultiStream = "md_uk_multi_stream"
    def ukDeleteBase = "md_uk_delete_base"
    def ukDeleteStream = "md_uk_delete_stream"
    def ukCrossRowsetBase = "md_uk_cross_rowset_base"
    def ukCrossRowsetStream = "md_uk_cross_rowset_stream"
    def ukCrossBlockBase = "md_uk_cross_block_base"
    def ukCrossBlockStream = "md_uk_cross_block_stream"
    def ukPendingBase = "md_uk_pending_base"
    def ukPendingStream = "md_uk_pending_stream"
    def ukDeleteBeforeBase = "md_uk_delete_before_base"
    def ukDeleteBeforeStream = "md_uk_delete_before_stream"
    def ukShowInitialBase = "md_uk_show_initial_base"
    def ukShowInitialStream = "md_uk_show_initial_stream"
    def ukShowInitialTarget = "md_uk_show_initial_target"
    def paperBase = "md_paper_base"
    def paperStream = "md_paper_stream"
    def incrBase = "md_incr_base"
    def incrDupBase = "md_incr_dup_base"
    def incrMowNoHistoryBase = "md_incr_mow_no_history_base"

    try {
        sql "DROP STREAM IF EXISTS ${ukStream}"
        sql "DROP TABLE IF EXISTS ${ukBase}"
        sql "DROP STREAM IF EXISTS ${ukSkipStream}"
        sql "DROP TABLE IF EXISTS ${ukSkipBase}"
        sql "DROP STREAM IF EXISTS ${ukMultiStream}"
        sql "DROP TABLE IF EXISTS ${ukMultiBase}"
        sql "DROP STREAM IF EXISTS ${ukDeleteStream}"
        sql "DROP TABLE IF EXISTS ${ukDeleteBase}"
        sql "DROP STREAM IF EXISTS ${ukCrossRowsetStream}"
        sql "DROP TABLE IF EXISTS ${ukCrossRowsetBase}"
        sql "DROP STREAM IF EXISTS ${ukCrossBlockStream}"
        sql "DROP TABLE IF EXISTS ${ukCrossBlockBase}"
        sql "DROP STREAM IF EXISTS ${ukPendingStream}"
        sql "DROP TABLE IF EXISTS ${ukPendingBase}"
        sql "DROP STREAM IF EXISTS ${ukDeleteBeforeStream}"
        sql "DROP TABLE IF EXISTS ${ukDeleteBeforeBase}"
        sql "DROP STREAM IF EXISTS ${ukShowInitialStream}"
        sql "DROP TABLE IF EXISTS ${ukShowInitialBase}"
        sql "DROP TABLE IF EXISTS ${ukShowInitialTarget}"
        sql "DROP STREAM IF EXISTS ${paperStream}"
        sql "DROP TABLE IF EXISTS ${paperBase}"
        sql "DROP TABLE IF EXISTS ${incrBase}"
        sql "DROP TABLE IF EXISTS ${incrDupBase}"
        sql "DROP TABLE IF EXISTS ${incrMowNoHistoryBase}"

        // 1) UNIQUE KEY + MIN_DELTA: verify UPDATE_BEFORE/UPDATE_AFTER are emitted as a pair.
        sql """
            CREATE TABLE ${ukBase} (
                id BIGINT,
                v1 INT
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
        sql "INSERT INTO ${ukBase} VALUES (1, 10)"
        sql """
            CREATE STREAM ${ukStream}
            ON TABLE ${ukBase}
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        sql "sync"
        sql "INSERT INTO ${ukBase} VALUES (1, 11)"
        sql "sync"
        sleep(1200)

        def ukRows = sql """
            SELECT id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM ${ukStream}
            ORDER BY id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
        """
        assertEquals(2, ukRows.size())
        assertEquals("1", ukRows[0][0].toString())
        assertEquals("10", ukRows[0][1].toString())
        assertEquals("UPDATE_BEFORE", ukRows[0][2].toString())
        assertEquals("1", ukRows[1][0].toString())
        assertEquals("11", ukRows[1][1].toString())
        assertEquals("UPDATE_AFTER", ukRows[1][2].toString())

        // 1.1) Stream hidden columns:
        // - __DORIS_STREAM_CHANGE_TYPE_COL__/__DORIS_STREAM_SEQUENCE_COL__ must be queryable;
        // - SELECT * should not expose hidden columns or __BEFORE__ columns by default.
        def ukMetaRows = sql """
            SELECT id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__, __DORIS_STREAM_SEQUENCE_COL__
            FROM ${ukStream}
            ORDER BY id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
        """
        assertEquals(2, ukMetaRows.size())
        assertEquals(false, ukMetaRows[0][3] == null)
        assertEquals(false, ukMetaRows[1][3] == null)

        def ukAllRows = sql """
            SELECT *
            FROM ${ukStream}
            ORDER BY id, v1
        """
        assertEquals(2, ukAllRows.size())
        // Base table has 2 visible columns; hidden columns and __BEFORE__v1__ must not be included by default.
        assertEquals(2, ukAllRows[0].size())
        assertEquals(2, ukAllRows[1].size())

        sql "SET show_hidden_columns=true"
        def ukDescRows = sql "DESC ${ukStream}"
        def hasBeforeCol = ukDescRows.any { it[0].toString().startsWith("__BEFORE__") }
        def hasChangeTypeCol = ukDescRows.any { it[0].toString().equalsIgnoreCase("__DORIS_STREAM_CHANGE_TYPE_COL__") }
        def hasSequenceCol = ukDescRows.any { it[0].toString().equalsIgnoreCase("__DORIS_STREAM_SEQUENCE_COL__") }
        assertEquals(false, hasBeforeCol)
        assertEquals(true, hasChangeTypeCol)
        assertEquals(true, hasSequenceCol)
        sql "SET show_hidden_columns=false"

        // 2) UNIQUE KEY + MIN_DELTA: verify INSERT then DELETE collapses to SKIP (empty result).
        sql """
            CREATE TABLE ${ukSkipBase} (
                id BIGINT,
                v1 INT
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
            CREATE STREAM ${ukSkipStream}
            ON TABLE ${ukSkipBase}
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        sql "INSERT INTO ${ukSkipBase} VALUES (2, 20)"
        sql "DELETE FROM ${ukSkipBase} WHERE id = 2"
        sql "sync"
        sleep(1200)

        def ukSkipRows = sql "SELECT id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__ FROM ${ukSkipStream}"
        assertEquals(0, ukSkipRows.size())

        // 3) UNIQUE KEY + MIN_DELTA: verify multiple UPDATEs on the same key keep only first/last as BEFORE/AFTER.
        sql """
            CREATE TABLE ${ukMultiBase} (
                id BIGINT,
                v1 INT
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
        sql "INSERT INTO ${ukMultiBase} VALUES (3, 30)"
        sql """
            CREATE STREAM ${ukMultiStream}
            ON TABLE ${ukMultiBase}
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        sql "INSERT INTO ${ukMultiBase} VALUES (3, 31)"
        sql "INSERT INTO ${ukMultiBase} VALUES (3, 32)"
        sql "sync"
        sleep(1200)

        def ukMultiRows = sql """
            SELECT id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM ${ukMultiStream}
            ORDER BY id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
        """
        assertEquals(2, ukMultiRows.size())
        assertEquals("3", ukMultiRows[0][0].toString())
        assertEquals("30", ukMultiRows[0][1].toString())
        assertEquals("UPDATE_BEFORE", ukMultiRows[0][2].toString())
        assertEquals("3", ukMultiRows[1][0].toString())
        assertEquals("32", ukMultiRows[1][1].toString())
        assertEquals("UPDATE_AFTER", ukMultiRows[1][2].toString())

        // 4) UNIQUE KEY + MIN_DELTA: verify a pure DELETE emits a single DELETE row.
        sql """
            CREATE TABLE ${ukDeleteBase} (
                id BIGINT,
                v1 INT
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
        sql "INSERT INTO ${ukDeleteBase} VALUES (4, 40)"
        sql """
            CREATE STREAM ${ukDeleteStream}
            ON TABLE ${ukDeleteBase}
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        sql "DELETE FROM ${ukDeleteBase} WHERE id = 4"
        sql "sync"
        sleep(1200)

        def ukDeleteRows = sql """
            SELECT id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM ${ukDeleteStream}
            ORDER BY id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
        """
        assertEquals(1, ukDeleteRows.size())
        assertEquals("4", ukDeleteRows[0][0].toString())
        assertEquals("40", ukDeleteRows[0][1].toString())
        assertEquals("DELETE", ukDeleteRows[0][2].toString())

        // 5) Same key across rowsets:
        // rowset-1 writes key=1(update) and key=2(insert), rowset-2 updates key=1 again.
        // Expect key=1 to fold into one UPDATE_BEFORE/UPDATE_AFTER pair; key=2 remains INSERT.
        sql """
            CREATE TABLE ${ukCrossRowsetBase} (
                id BIGINT,
                v1 INT
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
        // Seed old value for key=1 before stream starts.
        sql "INSERT INTO ${ukCrossRowsetBase} VALUES (1, 10)"
        sql """
            CREATE STREAM ${ukCrossRowsetStream}
            ON TABLE ${ukCrossRowsetBase}
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        // rowset-1
        sql "INSERT INTO ${ukCrossRowsetBase} VALUES (1, 11), (2, 30)"
        // rowset-2
        sql "INSERT INTO ${ukCrossRowsetBase} VALUES (1, 12)"
        sql "sync"
        sleep(1200)

        def ukCrossRowsetRows = sql """
            SELECT id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM ${ukCrossRowsetStream}
            ORDER BY id,
                CASE __DORIS_STREAM_CHANGE_TYPE_COL__
                    WHEN 'UPDATE_BEFORE' THEN 0
                    WHEN 'UPDATE_AFTER' THEN 1
                    WHEN 'DELETE' THEN 2
                    WHEN 'APPEND' THEN 3
                    ELSE 9
                END, v1
        """
        assertEquals(3, ukCrossRowsetRows.size())
        assertEquals("1", ukCrossRowsetRows[0][0].toString())
        assertEquals("10", ukCrossRowsetRows[0][1].toString())
        assertEquals("UPDATE_BEFORE", ukCrossRowsetRows[0][2].toString())
        assertEquals("1", ukCrossRowsetRows[1][0].toString())
        assertEquals("12", ukCrossRowsetRows[1][1].toString())
        assertEquals("UPDATE_AFTER", ukCrossRowsetRows[1][2].toString())
        assertEquals("2", ukCrossRowsetRows[2][0].toString())
        assertEquals("30", ukCrossRowsetRows[2][1].toString())
        assertEquals("APPEND", ukCrossRowsetRows[2][2].toString())

        def ukCrossRowsetSequenceRows = sql """
            SELECT id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__, __DORIS_STREAM_SEQUENCE_COL__
            FROM ${ukCrossRowsetStream}
            ORDER BY id,
                CASE __DORIS_STREAM_CHANGE_TYPE_COL__
                    WHEN 'UPDATE_BEFORE' THEN 0
                    WHEN 'UPDATE_AFTER' THEN 1
                    WHEN 'DELETE' THEN 2
                    WHEN 'APPEND' THEN 3
                    ELSE 9
                END, v1
        """
        assertTrue(ukCrossRowsetSequenceRows.every { it[3] != null })

        // 6) Same key across blocks: set batch_size=1 to force binlog rows of the same key into multiple blocks.
        // Expect only "first BEFORE image + last AFTER image" to be emitted.
        sql """
            CREATE TABLE ${ukCrossBlockBase} (
                id BIGINT,
                v1 INT
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
        sql "INSERT INTO ${ukCrossBlockBase} VALUES (7, 70)"
        sql """
            CREATE STREAM ${ukCrossBlockStream}
            ON TABLE ${ukCrossBlockBase}
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        sql "INSERT INTO ${ukCrossBlockBase} VALUES (7, 71)"
        sql "INSERT INTO ${ukCrossBlockBase} VALUES (7, 72)"
        sql "INSERT INTO ${ukCrossBlockBase} VALUES (7, 73)"
        sql "sync"
        sleep(1200)

        def ukCrossBlockRows = sql """
            SELECT /*+ SET_VAR(batch_size=1) */ id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM ${ukCrossBlockStream}
            ORDER BY id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
        """
        assertEquals(2, ukCrossBlockRows.size())
        assertEquals("7", ukCrossBlockRows[0][0].toString())
        assertEquals("70", ukCrossBlockRows[0][1].toString())
        assertEquals("UPDATE_BEFORE", ukCrossBlockRows[0][2].toString())
        assertEquals("7", ukCrossBlockRows[1][0].toString())
        assertEquals("73", ukCrossBlockRows[1][1].toString())
        assertEquals("UPDATE_AFTER", ukCrossBlockRows[1][2].toString())

        // 7) Emission across batch_size: UPDATE_BEFORE/UPDATE_AFTER may be split across two returns,
        // with the second row buffered in pending. Use batch_size=1 to verify no loss or reordering.
        sql """
            CREATE TABLE ${ukPendingBase} (
                id BIGINT,
                v1 INT
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
        sql "INSERT INTO ${ukPendingBase} VALUES (8, 80), (9, 90)"
        sql """
            CREATE STREAM ${ukPendingStream}
            ON TABLE ${ukPendingBase}
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        sql "INSERT INTO ${ukPendingBase} VALUES (8, 81)"
        sql "INSERT INTO ${ukPendingBase} VALUES (9, 91)"
        sql "sync"
        sleep(1200)

        def ukPendingRows = sql """
            SELECT /*+ SET_VAR(batch_size=1) */ id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM ${ukPendingStream}
            ORDER BY id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
        """
        assertEquals(4, ukPendingRows.size())
        assertEquals("8", ukPendingRows[0][0].toString())
        assertEquals("80", ukPendingRows[0][1].toString())
        assertEquals("UPDATE_BEFORE", ukPendingRows[0][2].toString())
        assertEquals("8", ukPendingRows[1][0].toString())
        assertEquals("81", ukPendingRows[1][1].toString())
        assertEquals("UPDATE_AFTER", ukPendingRows[1][2].toString())
        assertEquals("9", ukPendingRows[2][0].toString())
        assertEquals("90", ukPendingRows[2][1].toString())
        assertEquals("UPDATE_BEFORE", ukPendingRows[2][2].toString())
        assertEquals("9", ukPendingRows[3][0].toString())
        assertEquals("91", ukPendingRows[3][1].toString())
        assertEquals("UPDATE_AFTER", ukPendingRows[3][2].toString())

        // 8) UPDATE then DELETE: min_delta result is DELETE, and value columns should use __BEFORE__ (pre-delete snapshot).
        sql """
            CREATE TABLE ${ukDeleteBeforeBase} (
                id BIGINT,
                v1 INT
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
        sql "INSERT INTO ${ukDeleteBeforeBase} VALUES (10, 100)"
        sql """
            CREATE STREAM ${ukDeleteBeforeStream}
            ON TABLE ${ukDeleteBeforeBase}
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        sql "INSERT INTO ${ukDeleteBeforeBase} VALUES (10, 101)"
        sql "DELETE FROM ${ukDeleteBeforeBase} WHERE id = 10"
        sql "sync"
        sleep(1200)

        def ukDeleteBeforeRows = sql """
            SELECT id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM ${ukDeleteBeforeStream}
            ORDER BY id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
        """
        assertEquals(1, ukDeleteBeforeRows.size())
        assertEquals("10", ukDeleteBeforeRows[0][0].toString())
        assertEquals("101", ukDeleteBeforeRows[0][1].toString())
        assertEquals("DELETE", ukDeleteBeforeRows[0][2].toString())

        // 9) show_initial_rows=true:
        // direct stream query should first return historical rows only;
        // after consuming history, the stream should switch to incremental mode.
        sql """
            CREATE TABLE ${ukShowInitialBase} (
                id BIGINT,
                v1 INT,
                v2 VARCHAR(16)
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
            CREATE TABLE ${ukShowInitialTarget} (
                id BIGINT,
                v1 INT,
                v2 VARCHAR(16)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        sql "INSERT INTO ${ukShowInitialBase} VALUES (1, 11, 'a2'), (2, 11, 'a2'), (3, 11, 'a2')"
        sql """
            CREATE STREAM ${ukShowInitialStream}
            ON TABLE ${ukShowInitialBase}
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "true"
            )
        """
        sql "INSERT INTO ${ukShowInitialBase} VALUES (4, 11, 'a2')"
        sql "DELETE FROM ${ukShowInitialBase} WHERE id = 2"
        sql "sync"
        sleep(1200)

        def ukShowInitialRows = sql """
            SELECT id, __DORIS_STREAM_CHANGE_TYPE_COL__, __DORIS_STREAM_SEQUENCE_COL__
            FROM ${ukShowInitialStream}
            ORDER BY id, __DORIS_STREAM_CHANGE_TYPE_COL__
        """
        assertEquals(3, ukShowInitialRows.size())
        assertEquals("1", ukShowInitialRows[0][0].toString())
        assertEquals("APPEND", ukShowInitialRows[0][1].toString())
        assertEquals("2", ukShowInitialRows[1][0].toString())
        assertEquals("APPEND", ukShowInitialRows[1][1].toString())
        assertEquals("3", ukShowInitialRows[2][0].toString())
        assertEquals("APPEND", ukShowInitialRows[2][1].toString())
        assertTrue(ukShowInitialRows.every { it[2] != null })
        assertEquals(["-1"] as Set, ukShowInitialRows.collect { it[2].toString() }.toSet())

        sql "INSERT INTO ${ukShowInitialTarget} SELECT * FROM ${ukShowInitialStream}"

        def ukShowInitialIncrementalRows = sql """
            SELECT id, __DORIS_STREAM_CHANGE_TYPE_COL__, __DORIS_STREAM_SEQUENCE_COL__
            FROM ${ukShowInitialStream}
            ORDER BY id, __DORIS_STREAM_CHANGE_TYPE_COL__
        """
        assertEquals(2, ukShowInitialIncrementalRows.size())
        assertEquals("2", ukShowInitialIncrementalRows[0][0].toString())
        assertEquals("DELETE", ukShowInitialIncrementalRows[0][1].toString())
        assertEquals("4", ukShowInitialIncrementalRows[1][0].toString())
        assertEquals("APPEND", ukShowInitialIncrementalRows[1][1].toString())
        assertTrue(ukShowInitialIncrementalRows.every { it[2] != null })
        assertFalse(ukShowInitialIncrementalRows.collect { it[2].toString() }.toSet().contains("-1"))

        def ukShowInitialTargetRows = sql """
            SELECT id, v1, v2
            FROM ${ukShowInitialTarget}
            ORDER BY id
        """
        assertEquals(3, ukShowInitialTargetRows.size())
        assertEquals("1", ukShowInitialTargetRows[0][0].toString())
        assertEquals("2", ukShowInitialTargetRows[1][0].toString())
        assertEquals("3", ukShowInitialTargetRows[2][0].toString())

        // 10) Reproduce the paper's minimum-delta scenario (mixed INSERT/UPDATE/DELETE folding semantics):
        // - Walter: keep INSERT;
        // - Jeff -> Jeffrey: emit UPDATE_BEFORE/UPDATE_AFTER as a pair;
        // - Maud -> Maude (INSERT then UPDATE): fold into a single INSERT (latest value);
        // - Donny: keep DELETE;
        // - Uli (INSERT then DELETE): net change is empty and should be SKIP.
        sql """
            CREATE TABLE ${paperBase} (
                id BIGINT,
                name VARCHAR(32)
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
        sql "INSERT INTO ${paperBase} VALUES (1, 'Jeff'), (2, 'Donny')"
        sql """
            CREATE STREAM ${paperStream}
            ON TABLE ${paperBase}
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """

        sql "INSERT INTO ${paperBase} VALUES (3, 'Walter')"
        sql "INSERT INTO ${paperBase} VALUES (1, 'Jeffrey')"
        sql "INSERT INTO ${paperBase} VALUES (4, 'Maud')"
        sql "INSERT INTO ${paperBase} VALUES (4, 'Maude')"
        sql "DELETE FROM ${paperBase} WHERE id = 2"
        sql "INSERT INTO ${paperBase} VALUES (5, 'Uli')"
        sql "DELETE FROM ${paperBase} WHERE id = 5"
        sql "sync"

        def paperRows = sql """
            SELECT id, name, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM ${paperStream}
            ORDER BY id,
                CASE __DORIS_STREAM_CHANGE_TYPE_COL__
                    WHEN 'UPDATE_BEFORE' THEN 0
                    WHEN 'UPDATE_AFTER' THEN 1
                    WHEN 'DELETE' THEN 2
                    WHEN 'APPEND' THEN 3
                    ELSE 9
                END
        """
        assertEquals(5, paperRows.size())
        assertEquals("1", paperRows[0][0].toString())
        assertEquals("Jeff", paperRows[0][1].toString())
        assertEquals("UPDATE_BEFORE", paperRows[0][2].toString())
        assertEquals("1", paperRows[1][0].toString())
        assertEquals("Jeffrey", paperRows[1][1].toString())
        assertEquals("UPDATE_AFTER", paperRows[1][2].toString())
        assertEquals("2", paperRows[2][0].toString())
        assertEquals("Donny", paperRows[2][1].toString())
        assertEquals("DELETE", paperRows[2][2].toString())
        assertEquals("3", paperRows[3][0].toString())
        assertEquals("Walter", paperRows[3][1].toString())
        assertEquals("APPEND", paperRows[3][2].toString())
        assertEquals("4", paperRows[4][0].toString())
        assertEquals("Maude", paperRows[4][1].toString())
        assertEquals("APPEND", paperRows[4][2].toString())

        // 11) Base table @incr(timestamp-based) queries should support MIN_DELTA / APPEND_ONLY / DETAIL,
        // and DETAIL without startTimestamp should behave the same as default @incr().
        sql """
            CREATE TABLE ${incrBase} (
                id BIGINT,
                v1 INT
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
        sql "INSERT INTO ${incrBase} VALUES (1, 10), (3, 30)"
        sql "sync"

        sleep(1200)
        def incrTimeFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        def startTimestamp = incrTimeFormat.format(new Date())
        sleep(1200)

        sql "INSERT INTO ${incrBase} VALUES (2, 20)"
        sql "INSERT INTO ${incrBase} VALUES (1, 11)"
        sql "INSERT INTO ${incrBase} VALUES (1, 12)"
        sql "DELETE FROM ${incrBase} WHERE id = 3"
        sql "sync"

        sleep(1200)
        def endTimestamp = incrTimeFormat.format(new Date())
        sleep(1200)

        def minDeltaRows = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${incrBase}@incr('startTimestamp' = '${startTimestamp}',
                "endTimestamp" = "${endTimestamp}",
                "incrementType" =  "MIN_DELTA")
            ORDER BY id, __DORIS_BINLOG_OP__, v1
        """
        assertEquals(4, minDeltaRows.size())
        assertEquals("1", minDeltaRows[0][0].toString())
        assertEquals("10", minDeltaRows[0][1].toString())
        assertEquals("2", minDeltaRows[0][2].toString())
        assertEquals("1", minDeltaRows[1][0].toString())
        assertEquals("12", minDeltaRows[1][1].toString())
        assertEquals("3", minDeltaRows[1][2].toString())
        assertEquals("2", minDeltaRows[2][0].toString())
        assertEquals("20", minDeltaRows[2][1].toString())
        assertEquals("0", minDeltaRows[2][2].toString())
        assertEquals("3", minDeltaRows[3][0].toString())
        assertEquals("30", minDeltaRows[3][1].toString())
        assertEquals("1", minDeltaRows[3][2].toString())

        def appendOnlyRows = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${incrBase}@incr('startTimestamp' = '${startTimestamp}',
                "endTimestamp" = "${endTimestamp}",
                "incrementType" =  "APPEND_ONLY")
            ORDER BY id, __DORIS_BINLOG_OP__, v1
        """
        assertEquals(1, appendOnlyRows.size())
        assertEquals("2", appendOnlyRows[0][0].toString())
        assertEquals("20", appendOnlyRows[0][1].toString())
        assertEquals("0", appendOnlyRows[0][2].toString())

        def detailWithRangeRows = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${incrBase}@incr('startTimestamp' = '${startTimestamp}',
                "endTimestamp" = "${endTimestamp}",
                "incrementType" =  "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(6, detailWithRangeRows.size())
        assertEquals("2", detailWithRangeRows[0][0].toString())
        assertEquals("20", detailWithRangeRows[0][1].toString())
        assertEquals("0", detailWithRangeRows[0][2].toString())
        assertEquals("1", detailWithRangeRows[1][0].toString())
        assertEquals("10", detailWithRangeRows[1][1].toString())
        assertEquals("2", detailWithRangeRows[1][2].toString())
        assertEquals("1", detailWithRangeRows[2][0].toString())
        assertEquals("11", detailWithRangeRows[2][1].toString())
        assertEquals("3", detailWithRangeRows[2][2].toString())
        assertEquals("1", detailWithRangeRows[3][0].toString())
        assertEquals("11", detailWithRangeRows[3][1].toString())
        assertEquals("2", detailWithRangeRows[3][2].toString())
        assertEquals("1", detailWithRangeRows[4][0].toString())
        assertEquals("12", detailWithRangeRows[4][1].toString())
        assertEquals("3", detailWithRangeRows[4][2].toString())
        assertEquals("3", detailWithRangeRows[5][0].toString())
        assertEquals("30", detailWithRangeRows[5][1].toString())
        assertEquals("1", detailWithRangeRows[5][2].toString())

        def detailWithStartRows = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${incrBase}@incr('startTimestamp' = '${startTimestamp}',
                "incrementType" =  "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(detailWithRangeRows, detailWithStartRows)

        def detailDefaultRows = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${incrBase}@incr("incrementType" =  "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(8, detailDefaultRows.size())
        assertEquals("1", detailDefaultRows[0][0].toString())
        assertEquals("10", detailDefaultRows[0][1].toString())
        assertEquals("0", detailDefaultRows[0][2].toString())
        assertEquals("3", detailDefaultRows[1][0].toString())
        assertEquals("30", detailDefaultRows[1][1].toString())
        assertEquals("0", detailDefaultRows[1][2].toString())

        def emptyIncrRows = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${incrBase}@incr()
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(detailDefaultRows, emptyIncrRows)

        // 12) DETAIL incr should support duplicate table and MOW table without historical value.
        sql """
            CREATE TABLE ${incrDupBase} (
                id BIGINT,
                v1 INT
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "ROW"
            )
        """
        sql "INSERT INTO ${incrDupBase} VALUES (1, 10), (2, 20)"
        sql "sync"
        sleep(1200)
        def dupStartTimestamp = incrTimeFormat.format(new Date())
        sleep(1200)
        sql "INSERT INTO ${incrDupBase} VALUES (3, 30)"
        sql "sync"
        sleep(1200)

        def dupDetailRows = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${incrDupBase}@incr('startTimestamp' = '${dupStartTimestamp}',
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(1, dupDetailRows.size())
        assertEquals("3", dupDetailRows[0][0].toString())
        assertEquals("30", dupDetailRows[0][1].toString())
        assertEquals("0", dupDetailRows[0][2].toString())

        def dupMinDeltaRows =  sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${incrDupBase}@incr('startTimestamp' = '${dupStartTimestamp}',
                "incrementType" = "MIN_DELTA")
        """
        assertEquals(1, dupMinDeltaRows.size())
        assertEquals("3", dupMinDeltaRows[0][0].toString())
        assertEquals("30", dupMinDeltaRows[0][1].toString())
        assertEquals("0", dupMinDeltaRows[0][2].toString())

        sql """
            CREATE TABLE ${incrMowNoHistoryBase} (
                id BIGINT,
                v1 INT
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "false"
            )
        """
        sql "INSERT INTO ${incrMowNoHistoryBase} VALUES (1, 10)"
        sql "sync"
        sleep(1200)
        def mowNoHistoryStartTimestamp = incrTimeFormat.format(new Date())
        sleep(1200)
        sql "INSERT INTO ${incrMowNoHistoryBase} VALUES (1, 11)"
        sql "sync"
        sleep(1200)

        def mowNoHistoryDetailRows = sql """
            SELECT id, v1, __DORIS_BINLOG_OP__
            FROM ${incrMowNoHistoryBase}@incr('startTimestamp' = '${mowNoHistoryStartTimestamp}',
                "incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_LSN__
        """
        assertEquals(1, mowNoHistoryDetailRows.size())
        assertEquals("1", mowNoHistoryDetailRows[0][0].toString())
        assertEquals("11", mowNoHistoryDetailRows[0][1].toString())
        assertEquals("0", mowNoHistoryDetailRows[0][2].toString())

        test {
            sql """
                SELECT id, v1, __DORIS_BINLOG_OP__
                FROM ${incrMowNoHistoryBase}@incr('startTimestamp' = '${mowNoHistoryStartTimestamp}',
                    "incrementType" = "MIN_DELTA")
            """
            exception "MIN_DELTA INCR query requires base table to enable binlog.need_historical_value=true"
        }
    } finally {
        sql "DROP DATABASE IF EXISTS test_min_delta_stream_db"
    }
}
