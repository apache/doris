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

suite("test_ordered_compaction_hidden_version_column", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    setBeConfigTemporary([
            enable_ordered_data_compaction: true,
            ordered_data_compaction_min_segment_size: 1,
            segments_key_bounds_truncation_threshold: -1
    ]) {
        sql "DROP TABLE IF EXISTS test_ordered_compaction_hidden_version_column"
        sql """
            CREATE TABLE test_ordered_compaction_hidden_version_column (
                k INT NOT NULL,
                v INT NOT NULL
            ) UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false",
                "disable_auto_compaction" = "true")
        """

        sql "INSERT INTO test_ordered_compaction_hidden_version_column VALUES (10, 10), (11, 11)"
        sql "INSERT INTO test_ordered_compaction_hidden_version_column VALUES (20, 20), (21, 21)"
        sql "SET show_hidden_columns = true"
        def firstVersions = sql """SELECT __DORIS_VERSION_COL__
            FROM test_ordered_compaction_hidden_version_column WHERE k < 20 ORDER BY k"""
        def secondVersions = sql """SELECT __DORIS_VERSION_COL__
            FROM test_ordered_compaction_hidden_version_column WHERE k >= 20 ORDER BY k"""
        assert firstVersions.size() == 2 && secondVersions.size() == 2
        long v1 = firstVersions[0][0].toString().toLong()
        long v2 = secondVersions[0][0].toString().toLong()
        assert firstVersions.every { it[0].toString().toLong() == v1 }
        assert secondVersions.every { it[0].toString().toLong() == v2 }
        assert v1 > 0 && v2 == v1 + 1

        def tabletInfo = sql_return_maparray(
                "SHOW TABLETS FROM test_ordered_compaction_hidden_version_column").get(0)
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
            trigger_and_wait_compaction("test_ordered_compaction_hidden_version_column", "cumulative")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(
                    "SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets")
        }

        def metaUrl = sql_return_maparray(
                "SHOW TABLETS FROM test_ordered_compaction_hidden_version_column").get(0).MetaUrl
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
            FROM test_ordered_compaction_hidden_version_column ORDER BY k"""
        qt_min_max """SELECT MIN(__DORIS_VERSION_COL__) = ${v1},
            MAX(__DORIS_VERSION_COL__) = ${v2}
            FROM test_ordered_compaction_hidden_version_column"""
        qt_version_filters """SELECT
            (SELECT COUNT(*) FROM test_ordered_compaction_hidden_version_column
                WHERE __DORIS_VERSION_COL__ = ${v1}),
            (SELECT COUNT(*) FROM test_ordered_compaction_hidden_version_column
                WHERE __DORIS_VERSION_COL__ = ${v2})"""

        def featureEnabled = { String name ->
            def row = sql("SHOW FRONTEND CONFIG LIKE '%${name}'").find {
                it[0] == name || it[0] == "experimental_${name}"
            }
            return row != null && row[1] == "true"
        }
        if (featureEnabled("enable_feature_changelog") && featureEnabled("enable_feature_binlog")) {
            sql "DROP TABLE IF EXISTS test_ordered_compaction_hidden_tso_column"
            sql """
                CREATE TABLE test_ordered_compaction_hidden_tso_column (
                    k INT NOT NULL,
                    v INT NOT NULL
                ) DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "changelog.enable" = "true",
                    "disable_auto_compaction" = "true")
            """
            sql "INSERT INTO test_ordered_compaction_hidden_tso_column VALUES (10, 10), (11, 11)"
            sql "INSERT INTO test_ordered_compaction_hidden_tso_column VALUES (20, 20), (21, 21)"
            def firstTsos = sql """SELECT __DORIS_COMMIT_TSO_COL__
                FROM test_ordered_compaction_hidden_tso_column WHERE k < 20 ORDER BY k"""
            def secondTsos = sql """SELECT __DORIS_COMMIT_TSO_COL__
                FROM test_ordered_compaction_hidden_tso_column WHERE k >= 20 ORDER BY k"""
            assert firstTsos.size() == 2 && secondTsos.size() == 2
            long t1 = firstTsos[0][0].toString().toLong()
            long t2 = secondTsos[0][0].toString().toLong()
            assert t1 > 0 && t2 > t1
            assert firstTsos.every { it[0].toString().toLong() == t1 }
            assert secondTsos.every { it[0].toString().toLong() == t2 }

            def tsoTabletInfo = sql_return_maparray(
                    "SHOW TABLETS FROM test_ordered_compaction_hidden_tso_column").get(0)
            def (tsoInputCode, tsoInputBody, tsoInputError) = curl("GET", tsoTabletInfo.MetaUrl)
            assert tsoInputCode == 0 : tsoInputError
            def tsoInputRowsets = parseJson(tsoInputBody.trim()).rs_metas.findAll {
                it.start_version.toString().toLong() == it.end_version.toString().toLong() &&
                        it.num_rows.toString().toLong() == 2 &&
                        it.num_segments.toString().toLong() == 1
            }.sort { it.start_version.toString().toLong() }
            assert tsoInputRowsets.size() == 2
            long firstVersion = tsoInputRowsets[0].start_version.toString().toLong()
            long secondVersion = tsoInputRowsets[1].end_version.toString().toLong()
            assert secondVersion == firstVersion + 1
            try {
                GetDebugPoint().enableDebugPointForAllBEs(
                        "SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                        [tablet_id: "${tsoTabletInfo.TabletId}", start_version: "${firstVersion}",
                         end_version: "${secondVersion}"])
                trigger_and_wait_compaction("test_ordered_compaction_hidden_tso_column", "cumulative")
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(
                        "SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets")
            }

            def tsoMetaUrl = sql_return_maparray(
                    "SHOW TABLETS FROM test_ordered_compaction_hidden_tso_column").get(0).MetaUrl
            def (tsoCode, tsoBody, tsoError) = curl("GET", tsoMetaUrl)
            assert tsoCode == 0 : tsoError
            assert parseJson(tsoBody.trim()).rs_metas.count {
                it.start_version.toString().toLong() == firstVersion &&
                        it.end_version.toString().toLong() == secondVersion &&
                        it.num_rows.toString().toLong() == 4
            } == 1
            order_qt_commit_tso """SELECT k,
                (__DORIS_COMMIT_TSO_COL__ = CASE WHEN k < 20 THEN ${t1} ELSE ${t2} END) AS correct_tso
                FROM test_ordered_compaction_hidden_tso_column ORDER BY k"""
            order_qt_time_travel """SELECT k, v FROM test_ordered_compaction_hidden_tso_column
                FOR VERSION AS OF ${t1} ORDER BY k"""
        }
    }
}
