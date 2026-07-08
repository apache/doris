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

suite("test_search_score_cache", "p0") {
    def tableName = "search_score_cache_test"

    def assertPositiveScores = { result ->
        assertTrue(result.size() > 0)
        for (def row : result) {
            assertTrue(Double.parseDouble(row[1].toString()) > 0.0)
        }
    }

    def assertStablePositiveScoreQuery = { warmSql, scoreSql ->
        def warmResult1 = sql warmSql
        def warmResult2 = sql warmSql
        assertEquals(warmResult1, warmResult2)

        def scoreResult1 = sql scoreSql
        def scoreResult2 = sql scoreSql
        assertEquals(scoreResult1.size(), scoreResult2.size())
        assertEquals(scoreResult1.collect { it[0] as int }.sort(),
                scoreResult2.collect { it[0] as int }.sort())
        assertPositiveScores(scoreResult1)
        assertPositiveScores(scoreResult2)
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            status INT,
            title VARCHAR(200),
            content VARCHAR(500),
            v VARIANT,
            INDEX idx_title(title) USING INVERTED PROPERTIES(
                "parser" = "english",
                "support_phrase" = "true"
            ),
            INDEX idx_content(content) USING INVERTED PROPERTIES(
                "parser" = "english",
                "support_phrase" = "true"
            ),
            INDEX idx_v(v) USING INVERTED PROPERTIES(
                "parser" = "english",
                "support_phrase" = "true"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """INSERT INTO ${tableName} VALUES
        (1, 1, 'apple banana cherry', 'red fruit sweet apple', '{"host":"apple banana server"}'),
        (2, 1, 'apple banana date', 'fresh apple banana salad', '{"host":"apple server cluster"}'),
        (3, 0, 'banana grape mango', 'yellow fruit tropical', '{"host":"banana server"}'),
        (4, 1, 'apple grape kiwi', 'green fruit fresh apple', '{"host":"green apple server"}'),
        (5, 0, 'mango pineapple coconut', 'tropical fruit exotic', '{"host":"mango server"}'),
        (6, 1, 'apple cherry plum', 'mixed fruit apple salad', '{"host":"apple cherry node"}'),
        (7, 0, 'banana coconut papaya', 'smoothie blend tropical', '{"host":"banana coconut node"}'),
        (8, 1, 'grape cherry apple', 'wine fruit tart apple', '{"host":"grape apple node"}')
    """
    sql "sync"

    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_inverted_index_query_cache = true """

    // SEARCH DSL + score() must execute scorers even when the DSL bitmap cache is warm.
    assertStablePositiveScoreQuery(
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id
            FROM ${tableName}
            WHERE search('title:apple')
            ORDER BY id
        """,
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id, score() AS s
            FROM ${tableName}
            WHERE search('title:apple')
            ORDER BY s DESC
            LIMIT 10
        """
    )

    // MATCH_ANY + score() must bypass bitmap-only inverted index query cache.
    assertStablePositiveScoreQuery(
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id
            FROM ${tableName}
            WHERE title MATCH_ANY 'apple'
            ORDER BY id
        """,
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id, score() AS s
            FROM ${tableName}
            WHERE title MATCH_ANY 'apple'
            ORDER BY s DESC
            LIMIT 10
        """
    )

    // MATCH_ALL + score() should still produce collected BM25 scores after cache warmup.
    assertStablePositiveScoreQuery(
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id
            FROM ${tableName}
            WHERE title MATCH_ALL 'apple banana'
            ORDER BY id
        """,
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id, score() AS s
            FROM ${tableName}
            WHERE title MATCH_ALL 'apple banana'
            ORDER BY s DESC
            LIMIT 10
        """
    )

    // MATCH_PHRASE + score() should not materialize default 0 scores from cached bitmaps.
    assertStablePositiveScoreQuery(
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id
            FROM ${tableName}
            WHERE title MATCH_PHRASE 'apple banana'
            ORDER BY id
        """,
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id, score() AS s
            FROM ${tableName}
            WHERE title MATCH_PHRASE 'apple banana'
            ORDER BY s DESC
            LIMIT 10
        """
    )


    // score() with ordinary filters should still execute scorers after the indexed bitmap cache is warm.
    assertStablePositiveScoreQuery(
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id
            FROM ${tableName}
            WHERE title MATCH_ANY 'apple' AND status = 1
            ORDER BY id
        """,
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id, score() AS s
            FROM ${tableName}
            WHERE title MATCH_ANY 'apple' AND status = 1
            ORDER BY s DESC
            LIMIT 10
        """
    )

    // score() on VARIANT subfields must not fall back to default 0 after cache warmup.
    assertStablePositiveScoreQuery(
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id
            FROM ${tableName}
            WHERE CAST(v["host"] AS STRING) MATCH_PHRASE 'apple server'
            ORDER BY id
        """,
        """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id, score() AS s
            FROM ${tableName}
            WHERE CAST(v["host"] AS STRING) MATCH_PHRASE 'apple server'
            ORDER BY s DESC
            LIMIT 10
        """
    )

    // UNION ALL branches each have their own score() pushdown and must collect non-zero scores.
    def unionWarmSql = """
        (
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id
            FROM ${tableName}
            WHERE title MATCH_ANY 'apple'
            ORDER BY id
            LIMIT 3
        )
        UNION ALL
        (
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id
            FROM ${tableName}
            WHERE title MATCH_ANY 'banana'
            ORDER BY id
            LIMIT 3
        )
    """
    def unionScoreSql = """
        (
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id, score() AS s
            FROM ${tableName}
            WHERE title MATCH_ANY 'apple'
            ORDER BY s DESC
            LIMIT 3
        )
        UNION ALL
        (
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
                   id, score() AS s
            FROM ${tableName}
            WHERE title MATCH_ANY 'banana'
            ORDER BY s DESC
            LIMIT 3
        )
    """
    assertStablePositiveScoreQuery(unionWarmSql, unionScoreSql)

    sql "DROP TABLE IF EXISTS ${tableName}"
}
