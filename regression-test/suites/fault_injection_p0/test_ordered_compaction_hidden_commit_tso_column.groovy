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

// The base tablet of a row-format binlog DUPLICATE table carries the hidden COMMIT_TSO column,
// which every load stores as a zero placeholder that readers replace with the rowset commit TSO
// only for a single-version rowset. Its role is TABLET_ROLE_DATA, so it takes the normal ordered
// data compaction path, which must therefore not hard-link single-version load rowsets under a
// multi-version output; rowsets already rewritten by a compaction still take the link path and
// keep answering the exact commit TSO of every row, which `FOR VERSION AS OF` filters on.
suite("test_ordered_compaction_hidden_commit_tso_column", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def customBeConfig = [
        ordered_data_compaction_min_segment_size : 1,
        enable_ordered_data_compaction: true,
        segments_key_bounds_truncation_threshold: -1 // truncated key bounds would keep the inputs from being tidy
    ]
    setBeConfigTemporary(customBeConfig) {
        sql "set show_hidden_columns = true"
        sql """ DROP TABLE IF EXISTS test_ordered_compaction_hidden_commit_tso_column FORCE """
        sql """
            CREATE TABLE test_ordered_compaction_hidden_commit_tso_column (
                `k` int,
                `v` int
            ) engine=olap
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            properties(
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "disable_auto_compaction" = "true")
            """

        // four tidy single-version load rowsets: versions 2, 3, 4 and 5, each with its own commit TSO
        def loadTsos = []
        def load = { String values ->
            sql """ INSERT INTO test_ordered_compaction_hidden_commit_tso_column VALUES ${values} """
            loadTsos << (sql("SELECT MAX(__DORIS_COMMIT_TSO_COL__) FROM test_ordered_compaction_hidden_commit_tso_column")[0][0] as Long)
        }
        load("(10,10),(12,12),(14,14)")
        load("(20,20),(21,21),(22,22),(23,23),(24,24)")
        load("(30,30),(31,31)")
        load("(40,40),(41,41),(42,42)")
        logger.info("==== commit tso of each load: ${loadTsos}")
        assert loadTsos.toSet().size() == 4
        assert loadTsos.every { it > 0 }

        // A TSO is not stable across runs, so every row reports the load it belongs to (1..4, or 0
        // for a value that matches no load, which is what a linked zero placeholder would report).
        def loadOfEachRow = """
            select k, v,
                   case __DORIS_COMMIT_TSO_COL__
                        when ${loadTsos[0]} then 1 when ${loadTsos[1]} then 2
                        when ${loadTsos[2]} then 3 when ${loadTsos[3]} then 4
                        else 0 end as load_no
            from test_ordered_compaction_hidden_commit_tso_column order by k"""
        // the snapshot after the second load: rows of the first two loads only
        def snapshotAfterSecondLoad = """
            select k, v from test_ordered_compaction_hidden_commit_tso_column
            FOR VERSION AS OF ${loadTsos[1]} order by k"""

        qt_loads loadOfEachRow
        qt_loads_snapshot snapshotAfterSecondLoad

        // The companion row-binlog tablet is listed as well; the ordered compaction under test is
        // the one of the base tablet, whose role is TABLET_ROLE_DATA.
        def tablets = sql_return_maparray("show tablets from test_ordered_compaction_hidden_commit_tso_column")
        logger.info("==== tablets: ${tablets}")
        def baseTablets = tablets.findAll { it.IsRowBinlog.toString() == "false" }
        assert baseTablets.size() == 1
        def baseTablet = baseTablets.get(0)
        def tabletId = baseTablet.TabletId

        def rowsetSegmentRows = { long startVersion, long endVersion ->
            def (code, out, err) = curl("GET", baseTablet.MetaUrl)
            assert code == 0
            def jsonMeta = parseJson(out.trim())
            logger.info("==== tablet_meta.rs_metas: ${jsonMeta.rs_metas}")
            def rowset = jsonMeta.rs_metas.find { it.start_version == startVersion && it.end_version == endVersion }
            assert rowset != null
            return rowset.num_segment_rows
        }

        GetDebugPoint().clearDebugPointsForAllBEs()

        def do_cumu_compaction = { int start, int end ->
            GetDebugPoint().enableDebugPointForAllBEs("SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets", [tablet_id: "${tabletId}", start_version: "${start}", end_version: "${end}"])
            trigger_and_wait_compaction("test_ordered_compaction_hidden_commit_tso_column", "cumulative")
            GetDebugPoint().disableDebugPointForAllBEs("SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets")
        }

        try {
            // [2-2],[3-3] -> [2-3]: single-version inputs are rewritten into one segment, which
            // materializes the commit TSO of every row instead of linking the zero placeholders
            do_cumu_compaction(2, 3)
            assert rowsetSegmentRows(2, 3) == [8]
            qt_after_rewrite_2_3 loadOfEachRow
            qt_after_rewrite_2_3_snapshot snapshotAfterSecondLoad

            // [4-4],[5-5] -> [4-5]
            do_cumu_compaction(4, 5)
            assert rowsetSegmentRows(4, 5) == [5]
            qt_after_rewrite_4_5 loadOfEachRow
            qt_after_rewrite_4_5_snapshot snapshotAfterSecondLoad

            // Each cumulative compaction moves the cumulative point past its output, so
            // [2-3] and [4-5] are now base compaction input together with the empty [0-1].
            // [0-1],[2-3],[4-5] -> [0-5]: every input with data is multi-version, so ordered
            // compaction links their segments and the stored commit TSOs stay exact
            trigger_and_wait_compaction("test_ordered_compaction_hidden_commit_tso_column", "base")
            assert rowsetSegmentRows(0, 5) == [8, 5]
            qt_after_link_0_5 loadOfEachRow
            qt_after_link_0_5_snapshot snapshotAfterSecondLoad
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
