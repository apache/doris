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

suite("test_search_score_boolean_branches", "p0") {
    // SEARCH has no row-scan fallback. Keep its index path enabled under CI session fuzzing.
    sql "set enable_common_expr_pushdown = true"
    sql "set enable_inverted_index_query = true"

    sql "DROP TABLE IF EXISTS test_search_score_multi_column_mixed"
    sql """
        CREATE TABLE test_search_score_multi_column_mixed (
            id INT NOT NULL,
            title STRING NULL,
            content STRING NULL,
            tags STRING NULL,
            INDEX idx_title(title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content(content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_tags(tags) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default:1")
    """
    sql """
        INSERT INTO test_search_score_multi_column_mixed VALUES
            (1, 'apple banana', 'search engine', 'database analytics'),
            (2, 'apple guide', 'search basics', 'database'),
            (3, 'banana manual', 'engine tuning', 'analytics'),
            (4, 'apple banana', 'storage engine', 'database analytics'),
            (5, 'pear guide', 'search engine', 'database analytics'),
            (6, 'apple', 'engine search', 'analytics database'),
            (7, 'banana', 'search', 'database analytics'),
            (8, 'grape', 'storage', 'archive'),
            (9, 'apple banana apple', 'search engine search', 'database analytics'),
            (10, 'apple docs', 'search engine', 'database analytics archive')
    """

    order_qt_search """
        SELECT id, ROUND(score(), 4) AS bm25_score
        FROM test_search_score_multi_column_mixed
        WHERE search('(title:apple AND content:search) OR (tags:database AND tags:analytics)',
                     '{"default_operator":"AND","mode":"lucene","minimum_should_match":0}')
        ORDER BY score() DESC LIMIT 100
    """

    order_qt_match """
        SELECT id, ROUND(score(), 4) AS bm25_score
        FROM test_search_score_multi_column_mixed
        WHERE (title MATCH_ANY 'apple' AND content MATCH_ANY 'search')
           OR tags MATCH_ALL 'database analytics'
        ORDER BY score() DESC LIMIT 100
    """

    order_qt_match_warm """
        SELECT id, ROUND(score(), 4) AS bm25_score
        FROM test_search_score_multi_column_mixed
        WHERE (title MATCH_ANY 'apple' AND content MATCH_ANY 'search')
           OR tags MATCH_ALL 'database analytics'
        ORDER BY score() DESC LIMIT 100
    """

    order_qt_failed_and """
        SELECT id, ROUND(score(), 4) AS bm25_score
        FROM test_search_score_multi_column_mixed
        WHERE (title MATCH_ANY 'apple' AND content MATCH_ANY 'missing_token')
           OR tags MATCH_ALL 'database analytics'
        ORDER BY score() DESC LIMIT 100
    """

    order_qt_failed_and_reversed """
        SELECT id, ROUND(score(), 4) AS bm25_score
        FROM test_search_score_multi_column_mixed
        WHERE (content MATCH_ANY 'missing_token' AND title MATCH_ANY 'apple')
           OR tags MATCH_ALL 'database analytics'
        ORDER BY score() DESC LIMIT 100
    """

    order_qt_not_branch """
        SELECT id, ROUND(score(), 4) AS bm25_score
        FROM test_search_score_multi_column_mixed
        WHERE NOT (title MATCH_ANY 'apple') OR tags MATCH_ALL 'database analytics'
        ORDER BY score() DESC LIMIT 100
    """

    test {
        sql """
            SELECT id, score() AS bm25_score
            FROM test_search_score_multi_column_mixed
            WHERE (title MATCH_ANY 'apple' AND id > 0)
               OR tags MATCH_ALL 'database analytics'
            ORDER BY bm25_score DESC LIMIT 100
        """
        exception "Cannot score a compound predicate with incomplete index evaluation"
    }
}
