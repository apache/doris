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

suite("test_lance_search_snapshot", "p0,external") {
    /*
     * vector_search() and full_text_search() with the version, timestamp, tag and branch
     * properties, on a Lance filesystem catalog.
     *
     * search_snapshot.lance (see lance_build_search_snapshot.py), main chain:
     *   version 1  row_id 1..8     fragment 0      committed 2026-09-29 14:51:51.410834 UTC
     *   version 2  row_id 9..16    fragment 1      committed 2026-09-29 14:51:52.929307 UTC
     *   version 3  vec_idx over fragments 0, 1     committed 2026-09-29 14:51:54.457459 UTC
     *   version 4  body_idx over fragments 0, 1    committed 2026-09-29 14:51:55.985957 UTC
     *   version 5  row_id 17..24   fragment 2      committed 2026-09-29 14:51:57.507267 UTC
     *   version 6  delete row_id 3, 11             committed 2026-09-29 14:51:59.031043 UTC
     *   version 7  vec_idx rebuilt over 0, 1, 2    committed 2026-09-29 14:52:00.565680 UTC
     * Tag rel points at version 5. Branch dev forks from version 4; its version 5 appends
     * row_id 101..108 as fragment 2, and its version 6 rebuilds body_idx over all fragments.
     * Tag dev_rel points at dev version 5. Dev versions 4, 5 and 6 were committed at
     * 14:52:16.005995, 14:52:17.762386 and 14:52:19.576617 UTC, after every main version.
     * vec is [row_id, row_id + 1, row_id + 2, row_id + 3],
     * so the squared L2 distance between rows r and n is 4 * (n - r)^2, and nprobes=2 makes
     * the two-partition IVF_FLAT index exact. body is "doc <row_id> lance" for even row_ids and
     * "doc <row_id> doris" for odd ones.
     *
     * search_snapshot_pruned.lance lost version 1 to cleanup; version 2, pinned by tag kept,
     * still opens but its vector index files are gone; version 3 rebuilt the index.
     *
     * search_snapshot_evolved.lance changes its schema after its vector index: version 1 has
     * row_id 1..8 as fragment 0, version 2 builds vec_idx over it, version 3 adds note
     * ("note <row_id>"), version 4 renames vec to embedding (vec_idx still serves it), and
     * version 5 appends row_id 9..16 as fragment 1, which vec_idx does not cover.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_search_snapshot"
    String table = "${catalogName}.`default`.search_snapshot"
    String pruned = "${catalogName}.`default`.search_snapshot_pruned"
    String evolved = "${catalogName}.`default`.search_snapshot_evolved"
    def vectorSearch = { String selector, String query = "[0,1,2,3]", String topK = "3",
            String target = table ->
        """vector_search(
                "table"="${target}",
                "column"="vec",
                "query_vector"="${query}",
                "top_k"="${topK}",
                "metric"="l2",
                "nprobes"="2"${selector})"""
    }
    def fullTextSearch = { String selector, String coverageMode = "index_only", String query = "lance" ->
        """full_text_search(
                "table"="${table}",
                "column"="body",
                "query"="${query}",
                "top_k"="30",
                "coverage_mode"="${coverageMode}"${selector})"""
    }
    // Queries sit a quarter step past a row, so no two rows tie.
    def evolvedSearch = { String column, String selector, String query = "[2.25,3.25,4.25,5.25]" ->
        """vector_search(
                "table"="${evolved}",
                "column"="${column}",
                "query_vector"="${query}",
                "top_k"="3",
                "metric"="l2",
                "nprobes"="2"${selector})"""
    }
    def scannerV2Rows = sql """SHOW VARIABLES LIKE 'enable_file_scanner_v2'"""
    String originalScannerV2 = scannerV2Rows[0][1].toString()
    String originalTimeZone = (sql """SHOW VARIABLES LIKE 'time_zone'""")[0][1].toString()
    String originalLazy = (sql """SHOW VARIABLES LIKE 'enable_lance_lazy_materialization'""")[0][1].toString()

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    try {
        sql """SET enable_file_scanner_v2 = true"""
        sql """SET time_zone = 'UTC'"""
        sql """
            CREATE CATALOG `${catalogName}` PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "filesystem",
                "warehouse" = "s3://warehouse/lance",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "use_path_style" = "true"
            )
        """

        // Without a selector: the latest main version, where rows 3 and 11 are deleted.
        qt_latest """SELECT row_id, _distance FROM ${vectorSearch("")} ORDER BY _distance, row_id"""
        explain {
            sql("SELECT row_id FROM ${vectorSearch("")}")
            contains "lanceCatalogType=filesystem"
            contains "lanceVersion=7"
            contains "lanceManagedVersioning=false"
            notContains "lanceBranch="
            contains "lanceSearchIndexFragments=3"
            contains "lanceSearchUnindexedFragments=0"
        }

        // Before any index: a flat search over that version's two fragments.
        qt_version_2 """SELECT row_id, _distance FROM ${vectorSearch(', "version"="2"')} ORDER BY _distance, row_id"""
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "version"="2"')}")
            contains "lanceVersion=2"
            contains "lanceVectorIndexStatus=NO_MATCH"
            contains "lanceSearchIndexSegments=0"
            contains "lanceSearchUnindexedFragments=2"
        }
        test {
            sql """SELECT row_id FROM ${fullTextSearch(', "version"="3"')}"""
            exception "No committed Lance FTS index exists for column 'body' at dataset version 3"
        }

        // Version 5 appended fragment 2 after both indexes: the vector search combines the index
        // over fragments 0 and 1 with a flat search of fragment 2, and full-text search covers
        // only the indexed fragments.
        qt_version_5 """
            SELECT row_id, _distance FROM ${vectorSearch(', "version"="5"', "[17,18,19,20]")}
            ORDER BY _distance, row_id
        """
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "version"="5"')}")
            contains "lanceVersion=5"
            contains "lanceSearchIndexFragments=2"
            contains "lanceSearchUnindexedFragments=1"
        }
        order_qt_version_5_fts """SELECT row_id FROM ${fullTextSearch(', "version"="5"')}"""
        test {
            sql """SELECT row_id FROM ${fullTextSearch(', "version"="5"', "strict")}"""
            exception "requires every fragment at dataset version 5 to be indexed"
        }
        order_qt_version_4_fts_strict """SELECT row_id FROM ${fullTextSearch(', "version"="4"', "strict")}"""

        // Version 6 deleted rows 3 and 11 and still uses the original index; version 7 rebuilt it.
        // The query sits a quarter step past row 3, so no two rows tie.
        qt_version_6 """
            SELECT row_id, _distance FROM ${vectorSearch(', "version"="6"', "[3.25,4.25,5.25,6.25]")}
            ORDER BY _distance, row_id
        """
        qt_version_5_not_deleted """
            SELECT row_id, _distance FROM ${vectorSearch(', "version"="5"', "[3.25,4.25,5.25,6.25]")}
            ORDER BY _distance, row_id
        """
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "version"="6"')}")
            contains "lanceSearchIndexFragments=2"
            contains "lanceSearchUnindexedFragments=1"
        }
        // Full-text search sees the deletion too: rows 3 and 11 are "doris" rows.
        order_qt_version_5_fts_doris """SELECT row_id FROM ${fullTextSearch(', "version"="5"', "index_only", "doris")}"""
        order_qt_version_6_fts_doris """SELECT row_id FROM ${fullTextSearch(', "version"="6"', "index_only", "doris")}"""

        // use_index=false loads metadata without indexes, from the selected snapshot as well.
        qt_version_5_no_index """
            SELECT row_id, _distance FROM ${vectorSearch(', "version"="5", "use_index"="false"', "[17,18,19,20]")}
            ORDER BY _distance, row_id
        """
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "version"="5", "use_index"="false"')}")
            contains "lanceVersion=5"
            contains "lanceVectorIndexStatus=DISABLED"
            contains "lanceSearchUnindexedFragments=3"
        }

        // A tag and main written as a branch select main versions.
        qt_tag_rel """
            SELECT row_id, _distance FROM ${vectorSearch(', "tag"="rel"', "[17,18,19,20]")}
            ORDER BY _distance, row_id
        """
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "tag"="rel"')}")
            contains "lanceVersion=5"
            notContains "lanceBranch="
        }
        qt_main_branch """
            SELECT row_id, _distance FROM ${vectorSearch(', "branch"="main", "version"="2"')}
            ORDER BY _distance, row_id
        """

        // Branch dev: its version 6 shares main's version numbers and fragment ids, not its rows.
        qt_branch_latest """
            SELECT row_id, _distance FROM ${vectorSearch(', "branch"="dev"', "[100,101,102,103]")}
            ORDER BY _distance, row_id
        """
        qt_main_same_query """
            SELECT row_id, _distance FROM ${vectorSearch(', "version"="6"', "[100,101,102,103]")}
            ORDER BY _distance, row_id
        """
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "branch"="dev"')}")
            contains "lanceVersion=6"
            contains "lanceBranch=dev"
            contains "lanceSearchIndexFragments=2"
            contains "lanceSearchUnindexedFragments=1"
        }
        order_qt_branch_fts_strict """SELECT row_id FROM ${fullTextSearch(', "branch"="dev"', "strict")}"""
        qt_branch_no_index """
            SELECT row_id, _distance
            FROM ${vectorSearch(', "branch"="dev", "use_index"="false"', "[100,101,102,103]")}
            ORDER BY _distance, row_id
        """
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "branch"="dev", "use_index"="false"')}")
            contains "lanceVersion=6"
            contains "lanceBranch=dev"
            contains "lanceVectorIndexStatus=DISABLED"
        }
        qt_branch_fork """
            SELECT row_id, _distance FROM ${vectorSearch(', "branch"="dev", "version"="4"', "[100,101,102,103]")}
            ORDER BY _distance, row_id
        """
        // Historical branch versions past the fork differ from main's versions of the same number:
        // dev version 5 has row_id 101..108 where main version 5 has 17..24, and dev version 6
        // rebuilt body_idx over fragment 2, which main version 6 does not cover.
        qt_branch_version_5 """
            SELECT row_id, _distance FROM ${vectorSearch(', "branch"="dev", "version"="5"', "[100,101,102,103]")}
            ORDER BY _distance, row_id
        """
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "branch"="dev", "version"="5"')}")
            contains "lanceVersion=5"
            contains "lanceBranch=dev"
            contains "lanceSearchUnindexedFragments=1"
        }
        order_qt_branch_version_6_fts_strict """
            SELECT row_id FROM ${fullTextSearch(', "branch"="dev", "version"="6"', "strict")}
        """
        test {
            sql """SELECT row_id FROM ${fullTextSearch(', "version"="6"', "strict")}"""
            exception "requires every fragment at dataset version 6 to be indexed"
        }
        // dev_rel points at dev version 5, not the branch's latest; its FTS index covers only
        // the fork's fragments, and the error names the branch.
        order_qt_branch_tag_fts """SELECT row_id FROM ${fullTextSearch(', "tag"="dev_rel"')}"""
        test {
            sql """SELECT row_id FROM ${fullTextSearch(', "tag"="dev_rel"', "strict")}"""
            exception "dataset version 5 of branch 'dev' to be indexed"
        }

        // Timestamps in the session time zone: between versions 5 and 6 selects version 5.
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "timestamp"="2026-09-29 14:51:58"')}")
            contains "lanceVersion=5"
        }
        sql """SET time_zone = 'Asia/Shanghai'"""
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "timestamp"="2026-09-29 22:51:58.000"')}")
            contains "lanceVersion=5"
        }
        sql """SET time_zone = 'UTC'"""
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "branch"="dev", "timestamp"="2999-01-01 00:00:00"')}")
            contains "lanceVersion=6"
            contains "lanceBranch=dev"
        }
        // Between dev versions 5 and 6: ignoring the time would select dev version 6, and resolving
        // it on main would select main version 7.
        explain {
            sql("SELECT row_id FROM ${vectorSearch(', "branch"="dev", "timestamp"="2026-09-29 14:52:18"')}")
            contains "lanceVersion=5"
            contains "lanceBranch=dev"
        }
        test {
            sql """SELECT row_id FROM ${fullTextSearch(', "branch"="dev", "timestamp"="2026-09-29 14:52:18"', "strict")}"""
            exception "dataset version 5 of branch 'dev' to be indexed"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch(', "timestamp"="2026-09-29 14:51:51"')}"""
            exception "Lance table default.search_snapshot has no version at or before '2026-09-29 14:51:51'"
        }

        // Two-phase materialization reads the branch's rows, not main's fragment 2.
        sql """SET enable_lance_lazy_materialization = true"""
        String twoPhase = """
            SELECT row_id, body, _distance
            FROM ${vectorSearch(', "branch"="dev"', "[100,101,102,103]")}
            ORDER BY _distance
            LIMIT 3
        """
        explain {
            sql "verbose ${twoPhase}"
            contains "VMaterializeNode"
        }
        qt_branch_two_phase "${twoPhase}"
        qt_tag_two_phase """
            SELECT row_id, body, _distance
            FROM ${vectorSearch(', "tag"="rel"', "[100,101,102,103]")}
            ORDER BY _distance
            LIMIT 3
        """
        // The second phase fetches the payload by the column names of the planned version: vec
        // exists only up to version 3, note only from version 3 and embedding only from version 4,
        // so fetching from any other version fails.
        String evolvedTwoPhase = """
            SELECT row_id, embedding, note, body, _distance
            FROM ${evolvedSearch("embedding", ', "version"="4"')}
            ORDER BY _distance
            LIMIT 3
        """
        explain {
            sql "verbose ${evolvedTwoPhase}"
            contains "VMaterializeNode"
        }
        qt_evolved_two_phase "${evolvedTwoPhase}"
        String evolvedOldTwoPhase = """
            SELECT row_id, vec, note, body, _distance
            FROM ${evolvedSearch("vec", ', "version"="3"')}
            ORDER BY _distance
            LIMIT 3
        """
        explain {
            sql "verbose ${evolvedOldTwoPhase}"
            contains "VMaterializeNode"
        }
        qt_evolved_old_two_phase "${evolvedOldTwoPhase}"
        sql """SET enable_lance_lazy_materialization = ${originalLazy}"""

        // Schema changes: each search binds the column names, the index and the fragments of the
        // version it reads. The rename keeps the field id, so vec_idx serves embedding.
        qt_evolved_version_2 """
            SELECT row_id, _distance FROM ${evolvedSearch("vec", ', "version"="2"')} ORDER BY _distance, row_id
        """
        qt_evolved_version_4 """
            SELECT row_id, _distance FROM ${evolvedSearch("embedding", ', "version"="4"')}
            ORDER BY _distance, row_id
        """
        for (String version : ["2", "4"]) {
            String column = version == "2" ? "vec" : "embedding"
            explain {
                sql("SELECT row_id FROM ${evolvedSearch(column, ', "version"="' + version + '"')}")
                contains "lanceVersion=${version}"
                contains "lanceVectorColumn=${column}"
                contains "lanceVectorIndexStatus=USED"
                contains "lanceSearchIndexFragments=1"
                contains "lanceSearchUnindexedFragments=0"
            }
        }
        qt_evolved_latest """
            SELECT row_id, _distance FROM ${evolvedSearch("embedding", "", "[12.25,13.25,14.25,15.25]")}
            ORDER BY _distance, row_id
        """
        explain {
            sql("SELECT row_id FROM ${evolvedSearch("embedding", "")}")
            contains "lanceVersion=5"
            contains "lanceSearchIndexFragments=1"
            contains "lanceSearchUnindexedFragments=1"
        }
        test {
            sql """SELECT row_id FROM ${evolvedSearch("embedding", ', "version"="2"')}"""
            exception "Lance vector column 'embedding' does not exist"
        }
        test {
            sql """SELECT row_id FROM ${evolvedSearch("vec", ', "version"="4"')}"""
            exception "Lance vector column 'vec' does not exist"
        }
        test {
            sql """SELECT row_id, note FROM ${evolvedSearch("vec", ', "version"="2"')}"""
            exception "Unknown column 'note'"
        }

        // A selector that cannot be resolved fails; nothing falls back to the latest version.
        test {
            sql """SELECT row_id FROM ${vectorSearch(', "version"="99"')}"""
            exception "Lance version 99 of default.search_snapshot was not found"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch(', "tag"="nope"')}"""
            exception "Lance tag 'nope' of default.search_snapshot was not found"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch(', "branch"="nope"')}"""
            exception "Lance branch 'nope' of default.search_snapshot was not found"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch(', "branch"="dev", "version"="9"')}"""
            exception "Lance version 9 of default.search_snapshot@dev was not found"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch(', "version"="2", "timestamp"="2026-09-29 14:51:58"')}"""
            exception "'version' and 'timestamp' are mutually exclusive"
        }
        test {
            sql """SELECT row_id FROM ${fullTextSearch(', "tag"="rel", "branch"="dev"')}"""
            exception "'tag' and 'branch' are mutually exclusive"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch(', "version"="rel"')}"""
            exception "'version' must be a positive integer, but was 'rel'"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch(', "branch"=""')}"""
            exception "'branch' must not be empty"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch(', "timestamp"="1790693517507"')}"""
            exception "'timestamp' must be 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd HH:mm:ss.SSS'"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch("", "[0,1,2,3]", "3", table + "@branch(dev)")}"""
            exception "use the 'version', 'timestamp', 'tag' or 'branch' property"
        }

        // Cleanup: a removed version is missing, and a kept version whose index files are gone
        // fails at execution instead of searching another version.
        test {
            sql """SELECT row_id FROM ${vectorSearch(', "version"="1"', "[0,1,2,3]", "3", pruned)}"""
            exception "Lance version 1 of default.search_snapshot_pruned was not found"
        }
        test {
            sql """SELECT row_id FROM ${vectorSearch(', "tag"="kept"', "[0,1,2,3]", "3", pruned)}"""
            exception "at Lance dataset version 2 at s3://warehouse/lance/search_snapshot_pruned.lance failed"
        }
        qt_pruned_latest """
            SELECT row_id, _distance FROM ${vectorSearch("", "[0,1,2,3]", "3", pruned)}
            ORDER BY _distance, row_id
        """
    } finally {
        sql """SET enable_file_scanner_v2 = ${originalScannerV2}"""
        sql """SET time_zone = '${originalTimeZone}'"""
        sql """SET enable_lance_lazy_materialization = ${originalLazy}"""
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
