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

// The text indexes of a merge-on-write table give the rows written with the delete sign no terms
// and record them, so BM25 statistics leave those rows out without reading the delete-sign column.
// Each case compares the scores with those of a duplicate-key table loaded with the rows a query
// returns, on every index storage format: after the loads; after a cumulative compaction, which
// keeps the rows with the delete sign and carries their record over; after a full compaction,
// which drops them; and after BUILD INDEX on rows loaded before the index existed.
suite("test_bm25_score_delete_sign_statistics", "p0") {
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
    def createTable = { String table, String keys, String format, boolean withIndex ->
        def mergeOnWrite = keys.startsWith("UNIQUE") ? '"enable_unique_key_merge_on_write" = "true",' : ""
        def index = withIndex ? """, INDEX idx_content (content) USING INVERTED
                PROPERTIES("parser" = "english", "support_phrase" = "true")""" : ""
        sql "DROP TABLE IF EXISTS ${table}"
        sql """
            CREATE TABLE ${table} (
                id BIGINT,
                content TEXT NULL
                ${index}
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
    // Short documents, whose lengths the norms of every format hold exactly, and NULLs; rows a
    // later load overwrites, which the delete bitmap hides; and rows deleted with the delete sign,
    // as CDC loads and DELETE write them. Only these hold "gamma".
    def load = { String table ->
        sql """
            INSERT INTO ${table}
            SELECT number, if(number % 9 = 0, NULL,
                              concat_ws(' ', if(number % 3 = 0, 'jobs', 'work'),
                                        if(number % 4 = 0, 'try', 'rest'),
                                        repeat('alpha ', cast(number % 5 AS INT))))
            FROM numbers('number' = '600')
        """
        sql """
            INSERT INTO ${table}
            SELECT number * 5, concat_ws(' ', 'jobs again', repeat('beta ', cast(number % 4 AS INT)))
            FROM numbers('number' = '100')
        """
        sql "SET show_hidden_columns = true"
        sql """
            INSERT INTO ${table} (id, content, __DORIS_DELETE_SIGN__)
            SELECT number * 7 + 1, concat_ws(' ', 'jobs deleted', repeat('gamma ', cast(number % 3 AS INT))), 1
            FROM numbers('number' = '80')
        """
        sql "SET show_hidden_columns = false"
        sql "DELETE FROM ${table} WHERE id % 11 = 2"
    }
    def waitBuildIndex = { String table ->
        for (int waited = 0; waited < 300; ++waited) {
            def jobs = sql """SHOW BUILD INDEX WHERE TableName = "${table}" ORDER BY JobId"""
            if (jobs.size() > 0) {
                def state = jobs[jobs.size() - 1][7]
                assertTrue(state != "CANCELLED", "BUILD INDEX on ${table} was cancelled: ${jobs}")
                if (state == "FINISHED") {
                    return
                }
            }
            sleep(1000)
        }
        assertTrue(false, "BUILD INDEX on ${table} did not finish")
    }

    for (format in ["V2", "V3", "SNII"]) {
        def suffix = format.toLowerCase()
        def mow = "test_bm25_score_delete_sign_statistics_mow_${suffix}"
        def visible = "test_bm25_score_delete_sign_statistics_visible_${suffix}"
        createTable(mow, "UNIQUE KEY(id)", format, true)
        createTable(visible, "DUPLICATE KEY(id)", format, true)
        load(mow)
        sql "INSERT INTO ${visible} SELECT id, content FROM ${mow}"
        sql "sync"

        // A query that returns the rows written with the delete sign finds them by their values,
        // but the index gives them no terms, so a MATCH finds none of them.
        qt_delete_sign_rows_by_value """
            SELECT /*+ SET_VAR(skip_delete_sign = true) */ count(*)
            FROM ${mow} WHERE content LIKE '%gamma%'
        """
        qt_delete_sign_rows_by_match """
            SELECT /*+ SET_VAR(skip_delete_sign = true, enable_match_without_inverted_index = false) */
                count(*) FROM ${mow} WHERE content MATCH_ANY 'gamma'
        """
        // An untokenized index gives them no terms either, so = answered by the index finds none
        // of them, while the same predicate on the column finds them all. Their segment holds two
        // values, so that its zone map does not answer = on its own.
        def keyword = "test_bm25_score_delete_sign_statistics_keyword_${suffix}"
        sql "DROP TABLE IF EXISTS ${keyword}"
        sql """
            CREATE TABLE ${keyword} (
                id BIGINT,
                tag VARCHAR(32) NULL,
                INDEX idx_tag (tag) USING INVERTED
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "inverted_index_storage_format" = "${format}",
                "disable_auto_compaction" = "true"
            );
        """
        sql """
            INSERT INTO ${keyword}
            SELECT number, if(number % 2 = 0, 'kept', 'other') FROM numbers('number' = '200')
        """
        sql "SET show_hidden_columns = true"
        sql """
            INSERT INTO ${keyword} (id, tag, __DORIS_DELETE_SIGN__)
            SELECT number + 1000, if(number % 2 = 0, 'gone', 'spare'), 1 FROM numbers('number' = '40')
        """
        sql "SET show_hidden_columns = false"
        qt_keyword_by_index """
            SELECT /*+ SET_VAR(skip_delete_sign = true) */ count(*) FROM ${keyword} WHERE tag = 'gone'
        """
        qt_keyword_by_column """
            SELECT /*+ SET_VAR(skip_delete_sign = true, enable_inverted_index_query = false) */
                count(*) FROM ${keyword} WHERE tag = 'gone'
        """
        // For the same reason != answered by the index returns them all, whatever their value,
        // while on the column it returns only those whose value differs. Their segment holds two
        // values for the same reason.
        sql "SET show_hidden_columns = true"
        sql """
            INSERT INTO ${keyword} (id, tag, __DORIS_DELETE_SIGN__)
            SELECT number + 2000, if(number % 2 = 0, 'gone', 'spare'), 1 FROM numbers('number' = '20')
        """
        sql "SET show_hidden_columns = false"
        qt_keyword_not_equal_by_index """
            SELECT /*+ SET_VAR(skip_delete_sign = true) */ count(*) FROM ${keyword}
            WHERE tag != 'gone' AND id >= 2000
        """
        qt_keyword_not_equal_by_column """
            SELECT /*+ SET_VAR(skip_delete_sign = true, enable_inverted_index_query = false) */
                count(*) FROM ${keyword} WHERE tag != 'gone' AND id >= 2000
        """

        assertSameScores(scores(visible), scores(mow), "${format}, after the loads")
        trigger_and_wait_compaction(mow, "cumulative")
        assertSameScores(scores(visible), scores(mow), "${format}, after a cumulative compaction")
        trigger_and_wait_compaction(mow, "full")
        assertSameScores(scores(visible), scores(mow), "${format}, after a full compaction")

        if (!isCloudMode()) {
            // The index is built on rows already loaded, which BUILD INDEX reads with their delete
            // sign.
            def built = "test_bm25_score_delete_sign_statistics_built_${suffix}"
            createTable(built, "UNIQUE KEY(id)", format, false)
            load(built)
            sql """
                ALTER TABLE ${built} ADD INDEX idx_content (content) USING INVERTED
                PROPERTIES("parser" = "english", "support_phrase" = "true")
            """
            sql "BUILD INDEX idx_content ON ${built}"
            waitBuildIndex(built)
            assertSameScores(scores(visible), scores(built), "${format}, after BUILD INDEX")
        }
    }

    order_qt_scores """
        SELECT id, score() FROM test_bm25_score_delete_sign_statistics_mow_v3
        WHERE SEARCH('content:jobs') AND id < 60 ORDER BY score() DESC LIMIT 100
    """
}
