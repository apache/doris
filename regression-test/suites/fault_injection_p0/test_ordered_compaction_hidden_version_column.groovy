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

// A merge-on-read UNIQUE table carries the hidden VERSION column, which every load stores as a
// zero placeholder that readers replace with the rowset version only for a single-version rowset.
// Ordered data compaction must therefore not hard-link single-version load rowsets under a
// multi-version output; rowsets already rewritten by a compaction still take the link path and
// keep answering the exact version of every row.
suite("test_ordered_compaction_hidden_version_column", "nonConcurrent") {
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
        sql """ DROP TABLE IF EXISTS test_ordered_compaction_hidden_version_column """
        sql """
            CREATE TABLE test_ordered_compaction_hidden_version_column (
                `k` int,
                `v` int
            ) engine=olap
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            properties(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false",
                "disable_auto_compaction" = "true")
            """

        // four tidy single-version load rowsets: versions 2, 3, 4 and 5
        sql """ INSERT INTO test_ordered_compaction_hidden_version_column VALUES (10,10),(12,12),(14,14)"""
        sql """ INSERT INTO test_ordered_compaction_hidden_version_column VALUES (20,20),(21,21),(22,22),(23,23),(24,24)"""
        sql """ INSERT INTO test_ordered_compaction_hidden_version_column VALUES (30,30),(31,31)"""
        sql """ INSERT INTO test_ordered_compaction_hidden_version_column VALUES (40,40),(41,41),(42,42)"""
        qt_loads "select k, v, __DORIS_VERSION_COL__ from test_ordered_compaction_hidden_version_column order by k"

        def rowsetSegmentRows = { long startVersion, long endVersion ->
            def metaUrl = sql_return_maparray("show tablets from test_ordered_compaction_hidden_version_column").get(0).MetaUrl
            def (code, out, err) = curl("GET", metaUrl)
            assert code == 0
            def jsonMeta = parseJson(out.trim())
            logger.info("==== tablet_meta.rs_metas: ${jsonMeta.rs_metas}")
            def rowset = jsonMeta.rs_metas.find { it.start_version == startVersion && it.end_version == endVersion }
            assert rowset != null
            return rowset.num_segment_rows
        }

        def tabletId = sql_return_maparray("show tablets from test_ordered_compaction_hidden_version_column").get(0).TabletId

        GetDebugPoint().clearDebugPointsForAllBEs()

        def do_cumu_compaction = { int start, int end ->
            GetDebugPoint().enableDebugPointForAllBEs("SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets", [tablet_id: "${tabletId}", start_version: "${start}", end_version: "${end}"])
            trigger_and_wait_compaction("test_ordered_compaction_hidden_version_column", "cumulative")
            GetDebugPoint().disableDebugPointForAllBEs("SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets")
        }

        try {
            // [2-2],[3-3] -> [2-3]: single-version inputs are rewritten into one segment, which
            // materializes the version of every row instead of linking the zero placeholders
            do_cumu_compaction(2, 3)
            assert rowsetSegmentRows(2, 3) == [8]
            qt_after_rewrite_2_3 "select k, v, __DORIS_VERSION_COL__ from test_ordered_compaction_hidden_version_column order by k"

            // [4-4],[5-5] -> [4-5]
            do_cumu_compaction(4, 5)
            assert rowsetSegmentRows(4, 5) == [5]
            qt_after_rewrite_4_5 "select k, v, __DORIS_VERSION_COL__ from test_ordered_compaction_hidden_version_column order by k"

            // Each cumulative compaction moves the cumulative point past its output, so
            // [2-3] and [4-5] are now base compaction input together with the empty [0-1].
            // [0-1],[2-3],[4-5] -> [0-5]: every input with data is multi-version, so ordered
            // compaction links their segments and the stored versions stay exact
            trigger_and_wait_compaction("test_ordered_compaction_hidden_version_column", "base")
            assert rowsetSegmentRows(0, 5) == [8, 5]
            qt_after_link_0_5 "select k, v, __DORIS_VERSION_COL__ from test_ordered_compaction_hidden_version_column order by k"
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
