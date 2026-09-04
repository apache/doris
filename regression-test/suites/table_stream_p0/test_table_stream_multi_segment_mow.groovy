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

suite("test_table_stream_multi_segment_mow", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_table_stream_multi_segment_mow_db"
    sql "CREATE DATABASE test_table_stream_multi_segment_mow_db"
    sql "USE test_table_stream_multi_segment_mow_db"

    def customBeConfig = [
        doris_scanner_row_bytes: 1,
        // Section E drives row-binlog LMax quick-merge. Tiny thresholds let every small
        // rowset count as "compaction enough" so the cumulative point advances past two
        // LMax rowsets, and wait_timesec=0 stops freshly-visible singleton deltas from
        // being filtered out of the compaction candidate set. time_threshold is set huge
        // so the time-based trigger never preempts the quick-merge branch.
        binlog_compaction_goal_size_mbytes: 0,
        binlog_compaction_file_count_threshold: 2,
        binlog_compaction_wait_timesec_after_visible: 0,
        binlog_compaction_time_threshold_seconds: 86400
    ]

    setBeConfigTemporary(customBeConfig) {
        try {
            GetDebugPoint().clearDebugPointsForAllBEs()
            // shrink doris_scanner_row_bytes and enable MemTable.need_flush to make
            // each 5000-row batch produce multiple segments stably.
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")

            String batch1 = ""
            String batch2 = ""
            (1..5000).each { batch1 += "${it},${it}\n" }
            (5001..10000).each { batch2 += "${it},${it}\n" }


            // ========================================================
            // Section A. MoW + append_only.
            // ========================================================
            sql "DROP STREAM IF EXISTS ts_ms_mow_stream"
            sql "DROP TABLE IF EXISTS ts_ms_mow_base FORCE"
            sql """
                CREATE TABLE ts_ms_mow_base (
                    id BIGINT,
                    v INT
                ) ENGINE=OLAP
                UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "binlog.enable" = "true",
                    "binlog.format" = "ROW",
                    "binlog.need_historical_value" = "true"
                )
            """
            sql """
                CREATE STREAM ts_ms_mow_stream
                ON TABLE ts_ms_mow_base
                PROPERTIES (
                    "type" = "append_only",
                    "show_initial_rows" = "false"
                )
            """
            streamLoad {
                db "test_table_stream_multi_segment_mow_db"
                table "ts_ms_mow_base"
                set 'column_separator', ','
                set 'columns', 'id,v'
                inputStream new ByteArrayInputStream(batch1.getBytes())
                time 60000
                check { result, exception, startTime, endTime ->
                    if (exception != null) { throw exception }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            streamLoad {
                db "test_table_stream_multi_segment_mow_db"
                table "ts_ms_mow_base"
                set 'column_separator', ','
                set 'columns', 'id,v'
                inputStream new ByteArrayInputStream(batch2.getBytes())
                time 60000
                check { result, exception, startTime, endTime ->
                    if (exception != null) { throw exception }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            sql "sync"
            sleep(1200)

            order_qt_mow_count "SELECT COUNT(*) FROM ts_ms_mow_stream"
            order_qt_mow_change_types """
                SELECT __DORIS_STREAM_CHANGE_TYPE_COL__, COUNT(*)
                FROM ts_ms_mow_stream
                GROUP BY __DORIS_STREAM_CHANGE_TYPE_COL__
                ORDER BY __DORIS_STREAM_CHANGE_TYPE_COL__
            """
            order_qt_mow_sample """
                SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
                FROM ts_ms_mow_stream
                WHERE id IN (1, 5000, 5001, 10000)
                ORDER BY id
            """

            // ========================================================
            // Section B. MoW + append_only, overlapping batches.
            //   updates on existing keys are filtered, only brand-new keys append.
            //     batch1: id [1, 5000]        -> 5000 new keys, all APPEND
            //     batch2: id [2501, 7500]     -> [2501,5000] updates filtered,
            //                                    [5001,7500] 2500 new keys APPEND
            //     expect: 7500 APPEND rows.
            // ========================================================
            String overlapBatch1 = ""
            String overlapBatch2 = ""
            (1..5000).each { overlapBatch1 += "${it},${it}\n" }
            (2501..7500).each { overlapBatch2 += "${it},${it + 100000}\n" }

            sql "DROP STREAM IF EXISTS ts_ms_mow_ov_stream"
            sql "DROP TABLE IF EXISTS ts_ms_mow_ov_base FORCE"
            sql """
                CREATE TABLE ts_ms_mow_ov_base (
                    id BIGINT,
                    v INT
                ) ENGINE=OLAP
                UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "binlog.enable" = "true",
                    "binlog.format" = "ROW",
                    "binlog.need_historical_value" = "true"
                )
            """
            sql """
                CREATE STREAM ts_ms_mow_ov_stream
                ON TABLE ts_ms_mow_ov_base
                PROPERTIES (
                    "type" = "append_only",
                    "show_initial_rows" = "false"
                )
            """
            streamLoad {
                db "test_table_stream_multi_segment_mow_db"
                table "ts_ms_mow_ov_base"
                set 'column_separator', ','
                set 'columns', 'id,v'
                inputStream new ByteArrayInputStream(overlapBatch1.getBytes())
                time 60000
                check { result, exception, startTime, endTime ->
                    if (exception != null) { throw exception }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            streamLoad {
                db "test_table_stream_multi_segment_mow_db"
                table "ts_ms_mow_ov_base"
                set 'column_separator', ','
                set 'columns', 'id,v'
                inputStream new ByteArrayInputStream(overlapBatch2.getBytes())
                time 60000
                check { result, exception, startTime, endTime ->
                    if (exception != null) { throw exception }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            sql "sync"
            sleep(1200)

            qt_mow_ov_count "SELECT COUNT(*) FROM ts_ms_mow_ov_stream"
            order_qt_mow_ov_change_types """
                SELECT __DORIS_STREAM_CHANGE_TYPE_COL__, COUNT(*)
                FROM ts_ms_mow_ov_stream
                GROUP BY __DORIS_STREAM_CHANGE_TYPE_COL__
                ORDER BY __DORIS_STREAM_CHANGE_TYPE_COL__
            """
            order_qt_mow_ov_sample """
                SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
                FROM ts_ms_mow_ov_stream
                WHERE id IN (1, 2500, 2501, 5000, 5001, 7500)
                ORDER BY id
            """

            // ========================================================
            // Section C. MoW + min_delta, non-overlapping batches.
            //     batch1: id [1, 5000]      -> 5000 brand-new keys
            //     batch2: id [5001, 10000]  -> 5000 brand-new keys
            //     each key inserted once, nothing to fold -> 10000 APPEND rows.
            // ========================================================
            sql "DROP STREAM IF EXISTS ts_ms_mow_md_stream"
            sql "DROP TABLE IF EXISTS ts_ms_mow_md_base FORCE"
            sql """
                CREATE TABLE ts_ms_mow_md_base (
                    id BIGINT,
                    v INT
                ) ENGINE=OLAP
                UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "binlog.enable" = "true",
                    "binlog.format" = "ROW",
                    "binlog.need_historical_value" = "true"
                )
            """
            sql """
                CREATE STREAM ts_ms_mow_md_stream
                ON TABLE ts_ms_mow_md_base
                PROPERTIES (
                    "type" = "min_delta",
                    "show_initial_rows" = "false"
                )
            """
            streamLoad {
                db "test_table_stream_multi_segment_mow_db"
                table "ts_ms_mow_md_base"
                set 'column_separator', ','
                set 'columns', 'id,v'
                inputStream new ByteArrayInputStream(batch1.getBytes())
                time 60000
                check { result, exception, startTime, endTime ->
                    if (exception != null) { throw exception }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            streamLoad {
                db "test_table_stream_multi_segment_mow_db"
                table "ts_ms_mow_md_base"
                set 'column_separator', ','
                set 'columns', 'id,v'
                inputStream new ByteArrayInputStream(batch2.getBytes())
                time 60000
                check { result, exception, startTime, endTime ->
                    if (exception != null) { throw exception }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            sql "sync"
            sleep(1200)

            qt_mow_md_count "SELECT COUNT(*) FROM ts_ms_mow_md_stream"
            order_qt_mow_md_change_types """
                SELECT __DORIS_STREAM_CHANGE_TYPE_COL__, COUNT(*)
                FROM ts_ms_mow_md_stream
                GROUP BY __DORIS_STREAM_CHANGE_TYPE_COL__
                ORDER BY __DORIS_STREAM_CHANGE_TYPE_COL__
            """
            order_qt_mow_md_sample """
                SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
                FROM ts_ms_mow_md_stream
                WHERE id IN (1, 5000, 5001, 10000)
                ORDER BY id
            """

            // ========================================================
            // Section D. MoW + min_delta, overlapping batches.
            //   Combines Section B's overlapping layout with min_delta folding.
            //   The stream is created on an empty table (show_initial_rows=false),
            //   so there is no historical baseline to act as UPDATE_BEFORE. Every
            //   key's net change relative to the stream start is therefore a single
            //   APPEND carrying the latest value; an insert followed by an update
            //   folds to one APPEND(new value), NOT an UPDATE_BEFORE/UPDATE_AFTER
            //   pair. The point is that min_delta must keep every net-changed key
            //   without dropping events across segments.
            //     batch1: id [1, 5000]      v = id
            //     batch2: id [2501, 7500]   v = id + 100000
            //   expect: 7500 APPEND rows, overlapping keys carrying the latest value.
            //     [1, 2500]     inserted once           -> APPEND(id)
            //     [2501, 5000]  insert then update       -> APPEND(id + 100000)
            //     [5001, 7500]  inserted once           -> APPEND(id + 100000)
            // ========================================================
            sql "DROP STREAM IF EXISTS ts_ms_mow_md_ov_stream"
            sql "DROP TABLE IF EXISTS ts_ms_mow_md_ov_base FORCE"
            sql """
                CREATE TABLE ts_ms_mow_md_ov_base (
                    id BIGINT,
                    v INT
                ) ENGINE=OLAP
                UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "binlog.enable" = "true",
                    "binlog.format" = "ROW",
                    "binlog.need_historical_value" = "true"
                )
            """
            sql """
                CREATE STREAM ts_ms_mow_md_ov_stream
                ON TABLE ts_ms_mow_md_ov_base
                PROPERTIES (
                    "type" = "min_delta",
                    "show_initial_rows" = "false"
                )
            """
            streamLoad {
                db "test_table_stream_multi_segment_mow_db"
                table "ts_ms_mow_md_ov_base"
                set 'column_separator', ','
                set 'columns', 'id,v'
                inputStream new ByteArrayInputStream(overlapBatch1.getBytes())
                time 60000
                check { result, exception, startTime, endTime ->
                    if (exception != null) { throw exception }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            streamLoad {
                db "test_table_stream_multi_segment_mow_db"
                table "ts_ms_mow_md_ov_base"
                set 'column_separator', ','
                set 'columns', 'id,v'
                inputStream new ByteArrayInputStream(overlapBatch2.getBytes())
                time 60000
                check { result, exception, startTime, endTime ->
                    if (exception != null) { throw exception }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            sql "sync"
            sleep(1200)

            qt_mow_md_ov_count "SELECT COUNT(*) FROM ts_ms_mow_md_ov_stream"
            order_qt_mow_md_ov_change_types """
                SELECT __DORIS_STREAM_CHANGE_TYPE_COL__, COUNT(*)
                FROM ts_ms_mow_md_ov_stream
                GROUP BY __DORIS_STREAM_CHANGE_TYPE_COL__
                ORDER BY __DORIS_STREAM_CHANGE_TYPE_COL__
            """
            order_qt_mow_md_ov_sample """
                SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
                FROM ts_ms_mow_md_ov_stream
                WHERE id IN (1, 2500, 2501, 5000, 5001, 7500)
                ORDER BY id
            """

            // ========================================================
            // Section E. MoW + row-binlog LMax quick-merge.
            //   Sections A-D produce a single multi-segment rowset per load. This section
            //   targets the OTHER multi-segment source: binlog cumulative compaction's LMax
            //   quick-merge, which LINKS segments from several row-binlog rowsets into one
            //   non-singleton OVERLAPPING output rowset (compaction.cpp sets segments_overlap
            //   = OVERLAPPING for the quick-merge path). Because the same user key recurs in
            //   both merged LMax rowsets, after the link it lands in different segments of the
            //   one output rowset, each carrying a distinct binlog TSO. Reading it must NOT
            //   deduplicate by user key (is_unique=false on the row-binlog path); a UNIQUE
            //   dedup would drop same-key/different-TSO events. This reproduces that layout and
            //   asserts every event survives.
            //
            //   The BE binlog thresholds are lowered in customBeConfig so every small rowset is
            //   "compaction enough" and the cumulative point advances past two LMax rowsets.
            //   trigger_and_wait_compaction on the base table also drives the co-located hidden
            //   row-binlog tablet (SHOW TABLETS lists it; its compaction_policy="binlog"), so the
            //   cumulative trigger runs BinlogCumulativeCompactionPolicy on it. The load/trigger
            //   cadence mirrors test_binlog_compaction.groovy case2's Round1..Round7.
            // ========================================================
            sql "DROP STREAM IF EXISTS ts_ms_mow_qm_stream"
            sql "DROP TABLE IF EXISTS ts_ms_mow_qm_base FORCE"
            sql """
                CREATE TABLE ts_ms_mow_qm_base (
                    id BIGINT,
                    v INT
                ) ENGINE=OLAP
                UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "binlog.enable" = "true",
                    "binlog.format" = "ROW",
                    "binlog.need_historical_value" = "true"
                )
            """
            sql """
                CREATE STREAM ts_ms_mow_qm_stream
                ON TABLE ts_ms_mow_qm_base
                PROPERTIES (
                    "type" = "min_delta",
                    "show_initial_rows" = "false"
                )
            """

            // Build the FIRST LMax rowset [0-6]. Each INSERT is one binlog version.
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (1, 10), (2, 20)" // v1
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (1, 11)"          // v2
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (2, 21)"          // v3
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (3, 30)"          // v4
            // Round 1: Level0 [0-1],[2-2],[3-3],[4-4] -> Level1 [0-4].
            trigger_and_wait_compaction("ts_ms_mow_qm_base", "cumulative")
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (1, 12)"          // v5
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (2, 22)"          // v6
            // Round 2: Level0 [5-5],[6-6] -> Level1 [5-6].
            trigger_and_wait_compaction("ts_ms_mow_qm_base", "cumulative")
            // Round 3: Level1 [0-4],[5-6] -> Level2 (LMax) [0-6].
            trigger_and_wait_compaction("ts_ms_mow_qm_base", "cumulative")

            // Build the SECOND LMax rowset [7-12], reusing keys 1/2/3 so they recur across
            // both LMax rowsets (distinct TSOs), producing same-key rows in different segments
            // after the quick-merge link.
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (1, 13)"          // v7
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (2, 23)"          // v8
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (3, 31)"          // v9
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (1, 14)"          // v10
            // Round 4: Level0 [7-7],[8-8],[9-9],[10-10] -> Level1 [7-10].
            trigger_and_wait_compaction("ts_ms_mow_qm_base", "cumulative")
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (2, 24)"          // v11
            sql "INSERT INTO ts_ms_mow_qm_base VALUES (3, 32)"          // v12
            // Round 5: Level0 [11-11],[12-12] -> Level1 [11-12].
            trigger_and_wait_compaction("ts_ms_mow_qm_base", "cumulative")
            // Round 6: Level1 [7-10],[11-12] -> Level2 (LMax) [7-12].
            trigger_and_wait_compaction("ts_ms_mow_qm_base", "cumulative")

            // Round 7: Level2 {[0-6],[7-12]} are both before the cumulative point and both
            // compaction-enough (compact_enough_size=2>1) -> LMax quick-merge -> [0-12] with
            // segments_overlap=OVERLAPPING. This is the non-singleton overlapping rowset.
            trigger_and_wait_compaction("ts_ms_mow_qm_base", "cumulative")
            sql "sync"
            sleep(1200)

            // Guard: assert the quick-merge actually produced the target rowset before querying.
            // trigger_and_wait_compaction drives every tablet of the table via compact_type=
            // cumulative, including the co-located hidden row-binlog tablet (IsRowBinlog=true,
            // compaction_policy="binlog"), which BE routes to BinlogCumulativeCompactionPolicy +
            // LMax quick-merge. /api/compaction/show returns each rowset as
            //   "[start-end] <num_segments> DATA <OVERLAP> <rowset_id> <size> level=<n>"
            // (Rowset::get_rowset_info_str). The quick-merge signature (compaction.cpp: output
            // version = [front.start, back.end] with start_version==0 the trigger precondition,
            // and segments_overlap forced to OVERLAPPING) is: on the row-binlog tablet, a rowset
            // that starts at version 0, is OVERLAPPING, and links more than one segment. We match
            // that rather than a hardcoded end version, since the row-binlog tablet's version
            // range depends on the initial [0-1] rowset (here the merged rowset is [0-13]).
            // Without this guard, a compaction that never hit the quick-merge branch would
            // silently fall back to reading the original rowsets and the section could pass
            // without exercising the intended path.
            def qmBackendIdToIp = [:]
            def qmBackendIdToHttpPort = [:]
            getBackendIpHttpPort(qmBackendIdToIp, qmBackendIdToHttpPort)
            def qmTablets = sql_return_maparray "show tablets from ts_ms_mow_qm_base"
            def foundQuickMergeRowset = false
            for (qmTablet in qmTablets) {
                // The quick-merge OVERLAPPING rowset only lives on the hidden row-binlog tablet.
                if (qmTablet.IsRowBinlog != "true") {
                    continue
                }
                def qmBeHost = qmBackendIdToIp["${qmTablet.BackendId}"]
                def qmBePort = qmBackendIdToHttpPort["${qmTablet.BackendId}"]
                def (qmExitCode, qmStdout, qmStderr) =
                        be_show_tablet_status(qmBeHost, qmBePort, qmTablet.TabletId)
                assert qmExitCode == 0 : "show tablet status failed: ${qmStderr}"
                def qmStatus = parseJson(qmStdout.trim())
                logger.info("row-binlog tablet ${qmTablet.TabletId} policy=" +
                        "${qmStatus['cumulative policy type']} rowsets=${qmStatus.rowsets}")
                for (rowsetStr in qmStatus.rowsets) {
                    // e.g. "[0-13] 2 DATA OVERLAPPING <id> 2.80 KB level=0"
                    def fields = rowsetStr.trim().split(/\s+/)
                    if (fields.length >= 4 && fields[0].startsWith("[0-") &&
                            fields[1].isInteger() && (fields[1] as int) > 1 &&
                            fields[3] == "OVERLAPPING") {
                        foundQuickMergeRowset = true
                        logger.info("found LMax quick-merge rowset on row-binlog tablet " +
                                "${qmTablet.TabletId}: ${rowsetStr}")
                    }
                }
            }
            assert foundQuickMergeRowset :
                    "expected a non-singleton multi-segment OVERLAPPING row-binlog rowset starting " +
                    "at version 0 from LMax quick-merge, but none was found; row-binlog quick-merge " +
                    "did not run and Section E would read the original rowsets instead"

            // Ground truth for "no dropped events": the raw row-binlog op stream. This reads
            // the quick-merge OVERLAPPING rowset through the same row-binlog TabletReader path
            // (is_unique=false); a UNIQUE dedup would silently drop same-key/different-TSO rows,
            // shrinking this result.
            order_qt_mow_qm_binlog """
                SELECT __DORIS_BINLOG_OP__ AS op,
                       id,
                       v,
                       __BEFORE__v__
                FROM binlog("table" = "ts_ms_mow_qm_base")
                ORDER BY id, v, op
            """
            order_qt_mow_qm_count "SELECT COUNT(*) FROM ts_ms_mow_qm_stream"
            order_qt_mow_qm_change_types """
                SELECT __DORIS_STREAM_CHANGE_TYPE_COL__, COUNT(*)
                FROM ts_ms_mow_qm_stream
                GROUP BY __DORIS_STREAM_CHANGE_TYPE_COL__
                ORDER BY __DORIS_STREAM_CHANGE_TYPE_COL__
            """
            order_qt_mow_qm_sample """
                SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__
                FROM ts_ms_mow_qm_stream
                ORDER BY id, v
            """
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}