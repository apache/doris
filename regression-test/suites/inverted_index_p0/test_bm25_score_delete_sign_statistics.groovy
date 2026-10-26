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

// BM25 statistics of a merge-on-write table count the rows the scan returns. A query filters out
// the rows written with the delete sign, unless it sets skip_delete_sign or show_hidden_columns:
// then it returns and scores them, and the statistics must count them too. Each case compares the
// scores with those of a duplicate-key table loaded with the rows the query returns, on every
// index storage format.
suite("test_bm25_score_delete_sign_statistics", "p0") {
    def scores = { String table, String vars ->
        def rows = sql """
            SELECT /*+ SET_VAR(${vars}) */ id, score()
            FROM ${table}
            WHERE SEARCH('content:jobs OR content:try')
            ORDER BY score() DESC
            LIMIT 100000
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

    for (format in ["V2", "V3", "SNII"]) {
        def suffix = format.toLowerCase()
        def mow = "test_bm25_score_delete_sign_statistics_mow_${suffix}"
        def returned = "test_bm25_score_delete_sign_statistics_returned_${suffix}"
        def visible = "test_bm25_score_delete_sign_statistics_visible_${suffix}"
        for (table in [mow, returned, visible]) {
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

        // Short documents, whose lengths the norms of every format hold exactly, and NULLs.
        sql """
            INSERT INTO ${mow}
            SELECT number, if(number % 9 = 0, NULL,
                              concat_ws(' ', if(number % 3 = 0, 'jobs', 'work'),
                                        if(number % 4 = 0, 'try', 'rest'),
                                        repeat('alpha ', cast(number % 5 AS INT))))
            FROM numbers('number' = '600')
        """
        // Overwritten rows, which the delete bitmap hides whatever the query sets.
        sql """
            INSERT INTO ${mow}
            SELECT number * 5, concat_ws(' ', 'jobs again', repeat('beta ', cast(number % 4 AS INT)))
            FROM numbers('number' = '100')
        """
        // Rows deleted with the delete sign, as CDC loads write them, and by DELETE.
        sql "SET show_hidden_columns = true"
        sql """
            INSERT INTO ${mow} (id, content, __DORIS_DELETE_SIGN__)
            SELECT number * 7 + 1, concat_ws(' ', 'jobs deleted', repeat('gamma ', cast(number % 3 AS INT))), 1
            FROM numbers('number' = '80')
        """
        sql "SET show_hidden_columns = false"
        sql "DELETE FROM ${mow} WHERE id % 11 = 2"

        sql "SET skip_delete_sign = true"
        sql "INSERT INTO ${returned} SELECT id, content FROM ${mow}"
        sql "SET skip_delete_sign = false"
        sql "INSERT INTO ${visible} SELECT id, content FROM ${mow}"
        sql "sync"

        // The delete-sign rows are returned only when the query asks for them.
        def returnedRows = sql "SELECT count(*) FROM ${returned}"
        def visibleRows = sql "SELECT count(*) FROM ${visible}"
        assertTrue(returnedRows[0][0] > visibleRows[0][0])

        def expectedReturned = scores(returned, "use_fix_replica = -1")
        assertSameScores(expectedReturned, scores(mow, "skip_delete_sign = true"),
                "${format}, skip_delete_sign, compared with the returned rows")
        sql "SET show_hidden_columns = true"
        assertSameScores(expectedReturned, scores(mow, "use_fix_replica = -1"),
                "${format}, show_hidden_columns, compared with the returned rows")
        sql "SET show_hidden_columns = false"
        assertSameScores(scores(visible, "use_fix_replica = -1"), scores(mow, "use_fix_replica = -1"),
                "${format}, default, compared with the visible rows")
    }

    // The scores a query returns with each setting, on one format.
    order_qt_default """
        SELECT id, score() FROM test_bm25_score_delete_sign_statistics_mow_v3
        WHERE SEARCH('content:jobs') AND id < 60 ORDER BY score() DESC LIMIT 100
    """
    order_qt_skip_delete_sign """
        SELECT /*+ SET_VAR(skip_delete_sign = true) */ id, score()
        FROM test_bm25_score_delete_sign_statistics_mow_v3
        WHERE SEARCH('content:jobs') AND id < 60 ORDER BY score() DESC LIMIT 100
    """
    sql "SET show_hidden_columns = true"
    order_qt_show_hidden_columns """
        SELECT id, score() FROM test_bm25_score_delete_sign_statistics_mow_v3
        WHERE SEARCH('content:jobs') AND id < 60 ORDER BY score() DESC LIMIT 100
    """
    // A query that filters the delete sign itself leaves the rows out, and the statistics too.
    order_qt_show_hidden_columns_filtered """
        SELECT id, score() FROM test_bm25_score_delete_sign_statistics_mow_v3
        WHERE SEARCH('content:jobs') AND id < 60 AND __DORIS_DELETE_SIGN__ = 0
        ORDER BY score() DESC LIMIT 100
    """
    sql "SET show_hidden_columns = false"
}
