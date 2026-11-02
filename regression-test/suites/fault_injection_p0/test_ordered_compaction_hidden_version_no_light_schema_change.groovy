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

suite("test_ordered_compaction_hidden_version_no_light_schema_change", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    setBeConfigTemporary([
            enable_ordered_data_compaction: true,
            ordered_data_compaction_min_segment_size: 1,
            segments_key_bounds_truncation_threshold: -1
    ]) {
        sql "DROP TABLE IF EXISTS test_ordered_compaction_hidden_version_no_light_schema_change"
        sql """
            CREATE TABLE test_ordered_compaction_hidden_version_no_light_schema_change (
                k INT NOT NULL,
                v INT NOT NULL
            ) UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false",
                "light_schema_change" = "false",
                "disable_auto_compaction" = "true")
        """

        sql "INSERT INTO test_ordered_compaction_hidden_version_no_light_schema_change VALUES (10, 10), (11, 11)"
        sql "INSERT INTO test_ordered_compaction_hidden_version_no_light_schema_change VALUES (20, 20), (21, 21)"
        sql "SET show_hidden_columns = true"
        def firstVersions = sql """SELECT __DORIS_VERSION_COL__
            FROM test_ordered_compaction_hidden_version_no_light_schema_change WHERE k < 20 ORDER BY k"""
        def secondVersions = sql """SELECT __DORIS_VERSION_COL__
            FROM test_ordered_compaction_hidden_version_no_light_schema_change WHERE k >= 20 ORDER BY k"""
        assert firstVersions.size() == 2 && secondVersions.size() == 2
        long v1 = firstVersions[0][0].toString().toLong()
        long v2 = secondVersions[0][0].toString().toLong()
        assert firstVersions.every { it[0].toString().toLong() == v1 }
        assert secondVersions.every { it[0].toString().toLong() == v2 }
        assert v1 > 0 && v2 == v1 + 1

        def tabletInfo = sql_return_maparray(
                "SHOW TABLETS FROM test_ordered_compaction_hidden_version_no_light_schema_change").get(0)
        def tabletId = tabletInfo.TabletId
        def (inputCode, inputBody, inputError) = curl("GET", tabletInfo.MetaUrl)
        assert inputCode == 0 : inputError
        def inputRowsets = parseJson(inputBody.trim()).rs_metas
        for (def version : [v1, v2]) {
            assert inputRowsets.count {
                it.start_version.toString().toLong() == version &&
                        it.end_version.toString().toLong() == version &&
                        it.num_rows.toString().toLong() == 2 &&
                        it.num_segments.toString().toLong() == 1
            } == 1
        }
        try {
            GetDebugPoint().enableDebugPointForAllBEs(
                    "SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                    [tablet_id: "${tabletId}", start_version: "${v1}", end_version: "${v2}"])
            trigger_and_wait_compaction("test_ordered_compaction_hidden_version_no_light_schema_change", "cumulative")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(
                    "SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets")
        }

        def metaUrl = sql_return_maparray(
                "SHOW TABLETS FROM test_ordered_compaction_hidden_version_no_light_schema_change").get(0).MetaUrl
        def (code, body, error) = curl("GET", metaUrl)
        assert code == 0 : error
        def rowsets = parseJson(body.trim()).rs_metas
        assert rowsets.count {
            it.start_version.toString().toLong() == v1 &&
                    it.end_version.toString().toLong() == v2 &&
                    it.num_rows.toString().toLong() == 4
        } == 1

        order_qt_versions """SELECT k,
            (__DORIS_VERSION_COL__ = CASE WHEN k < 20 THEN ${v1} ELSE ${v2} END) AS correct_version
            FROM test_ordered_compaction_hidden_version_no_light_schema_change ORDER BY k"""
        qt_min_max """SELECT MIN(__DORIS_VERSION_COL__) = ${v1},
            MAX(__DORIS_VERSION_COL__) = ${v2}
            FROM test_ordered_compaction_hidden_version_no_light_schema_change"""
        qt_version_filters """SELECT
            (SELECT COUNT(*) FROM test_ordered_compaction_hidden_version_no_light_schema_change
                WHERE __DORIS_VERSION_COL__ = ${v1}),
            (SELECT COUNT(*) FROM test_ordered_compaction_hidden_version_no_light_schema_change
                WHERE __DORIS_VERSION_COL__ = ${v2})"""

    }
}
