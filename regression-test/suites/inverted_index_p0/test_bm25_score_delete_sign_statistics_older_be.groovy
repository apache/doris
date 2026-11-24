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

// A BE that predates it neither leaves the rows written with the delete sign out of a text index
// nor records them. Its segments stay after an upgrade, and BM25 statistics read their delete-sign
// column instead. A compaction that merges such a segment's index with others records none for the
// result, which BM25 statistics then read the column for too. The debug point has this BE write
// indexes as an older one does; the scores must match a duplicate-key table holding the rows the
// query returns, before and after each compaction, on every index storage format.
suite("test_bm25_score_delete_sign_statistics_older_be", "nonConcurrent") {
    def scores = { String table ->
        def rows = sql """
            SELECT id, score()
            FROM ${table}
            WHERE SEARCH('content:jobs OR content:try') ORDER BY score() DESC LIMIT 100000
        """
        def byId = [:]
        rows.each { byId[it[0]] = it[1] }
        return byId
    }
    def assertSameScores = { Map expected, Map actual, String state ->
        assertTrue(expected.size() > 0)
        assertEquals(expected.size(), actual.size())
        def differing = expected.findAll { id, score -> actual[id] != score }
        assertTrue(differing.isEmpty(),
                "${state}: ${differing.size()} rows score differently, "
                        + "e.g. ${differing.take(3).collect { id, score -> "id ${id}: ${actual[id]} vs ${score}" }}")
    }
    def olderBe = "VerticalSegmentWriter._records_delete_sign_rows.disable"

    for (format in ["V2", "V3", "SNII"]) {
        def suffix = format.toLowerCase()
        def mow = "test_bm25_score_delete_sign_older_be_mow_${suffix}"
        def visible = "test_bm25_score_delete_sign_older_be_visible_${suffix}"
        for (table in [mow, visible]) {
            def keys = table == mow ? "UNIQUE KEY(id)" : "DUPLICATE KEY(id)"
            def mergeOnWrite = table == mow ? '"enable_unique_key_merge_on_write" = "true",' : ""
            sql "DROP TABLE IF EXISTS ${table}"
            sql """
                CREATE TABLE ${table} (
                    id BIGINT,
                    content TEXT NULL,
                    INDEX idx_content (content) USING INVERTED
                        PROPERTIES("parser" = "english", "support_phrase" = "true")
                ) ENGINE=OLAP
                ${keys}
                DISTRIBUTED BY HASH(id) BUCKETS 2
                PROPERTIES (
                    "replication_num" = "1",
                    ${mergeOnWrite}
                    "inverted_index_storage_format" = "${format}",
                    "disable_auto_compaction" = "true"
                );
            """
        }

        // Written as an older BE writes them: rows, and rows deleted with the delete sign, whose
        // terms the index keeps.
        try {
            GetDebugPoint().enableDebugPointForAllBEs(olderBe)
            sql """
                INSERT INTO ${mow}
                SELECT number, if(number % 9 = 0, NULL,
                                  concat_ws(' ', if(number % 3 = 0, 'jobs', 'work'),
                                            if(number % 4 = 0, 'try', 'rest'),
                                            repeat('alpha ', cast(number % 5 AS INT))))
                FROM numbers('number' = '600')
            """
            sql "SET show_hidden_columns = true"
            sql """
                INSERT INTO ${mow} (id, content, __DORIS_DELETE_SIGN__)
                SELECT number * 7 + 1, concat_ws(' ', 'jobs deleted', repeat('gamma ', cast(number % 3 AS INT))), 1
                FROM numbers('number' = '40')
            """
            sql "SET show_hidden_columns = false"
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(olderBe)
        }
        // Written after the upgrade: overwritten rows and more rows with the delete sign.
        sql """
            INSERT INTO ${mow}
            SELECT number * 5, concat_ws(' ', 'jobs again', repeat('beta ', cast(number % 4 AS INT)))
            FROM numbers('number' = '100')
        """
        sql "SET show_hidden_columns = true"
        sql """
            INSERT INTO ${mow} (id, content, __DORIS_DELETE_SIGN__)
            SELECT number * 7 + 281, concat_ws(' ', 'jobs deleted', repeat('try ', cast(number % 3 AS INT))), 1
            FROM numbers('number' = '40')
        """
        sql "SET show_hidden_columns = false"
        sql "DELETE FROM ${mow} WHERE id % 11 = 2"
        sql "INSERT INTO ${visible} SELECT id, content FROM ${mow}"
        sql "sync"

        // The older segment's rows with the delete sign still hold their terms.
        qt_older_delete_sign_rows_by_match """
            SELECT /*+ SET_VAR(skip_delete_sign = true, enable_match_without_inverted_index = false) */
                count(*) FROM ${mow} WHERE content MATCH_ANY 'gamma'
        """

        assertSameScores(scores(visible), scores(mow), "${format}, older and newer segments")
        trigger_and_wait_compaction(mow, "cumulative")
        assertSameScores(scores(visible), scores(mow),
                "${format}, after a cumulative compaction of older and newer segments")
        trigger_and_wait_compaction(mow, "full")
        assertSameScores(scores(visible), scores(mow), "${format}, after a full compaction")
    }
}
