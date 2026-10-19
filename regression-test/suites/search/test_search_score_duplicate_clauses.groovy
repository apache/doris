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

suite("test_search_score_duplicate_clauses", "p0") {
    sql "set enable_common_expr_pushdown=true"
    sql "set enable_inverted_index_query=true"
    sql "DROP TABLE IF EXISTS test_search_duplicate_clause_score"
    sql """
        CREATE TABLE test_search_duplicate_clause_score (
            id INT NOT NULL,
            content STRING NULL,
            INDEX idx_content(content) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default:1")
    """
    sql """
        INSERT INTO test_search_duplicate_clause_score VALUES
            (1, 'spider spider man'),
            (2, 'spider web'),
            (3, 'spider man and spider woman'),
            (4, 'man and web'),
            (5, 'database search engine')
    """

    qt_search_single1 """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE search('(content:spider)', '{"default_operator":"AND","mode":"lucene","minimum_should_match":0}')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_search_or2 """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE search('(content:spider) OR (content:spider)', '{"default_operator":"AND","mode":"lucene","minimum_should_match":0}')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_search_or3 """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE search('(content:spider) OR (content:spider) OR (content:spider)', '{"default_operator":"AND","mode":"lucene","minimum_should_match":0}')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_search_and2 """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE search('(content:spider) AND (content:spider)', '{"default_operator":"AND","mode":"lucene","minimum_should_match":0}')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_search_and3 """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE search('(content:spider) AND (content:spider) AND (content:spider)', '{"default_operator":"AND","mode":"lucene","minimum_should_match":0}')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_match_single1 """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_match_or2 """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider') OR (content MATCH_ANY 'spider')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_match_or3 """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider') OR (content MATCH_ANY 'spider') OR (content MATCH_ANY 'spider')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_match_and2 """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider') AND (content MATCH_ANY 'spider')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_match_and3 """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider') AND (content MATCH_ANY 'spider') AND (content MATCH_ANY 'spider')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_search_mixed """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE search('content:spider OR content:spider OR content:web')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_match_mixed """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider') OR (content MATCH_ANY 'spider') OR (content MATCH_ANY 'web')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_search_nested """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE search('(content:spider AND content:web) OR (content:spider AND content:web)')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_match_nested """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider' AND content MATCH_ANY 'web') OR (content MATCH_ANY 'spider' AND content MATCH_ANY 'web')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_search_nested_and """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE search('(content:spider OR content:web) AND (content:spider OR content:web)')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_match_nested_and """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider' OR content MATCH_ANY 'web')
            AND (content MATCH_ANY 'spider' OR content MATCH_ANY 'web')
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_match_order_by_score_only """
        SELECT id FROM test_search_duplicate_clause_score
        WHERE content MATCH_ANY 'spider' OR content MATCH_ANY 'spider' OR content MATCH_ANY 'web'
        ORDER BY score() DESC LIMIT 100
    """

    qt_match_derived """
        SELECT id, score() AS bm25_score
        FROM (
            SELECT id, content FROM test_search_duplicate_clause_score
            WHERE content MATCH_ANY 'spider' OR content MATCH_ANY 'spider'
        ) t
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_match_cte """
        WITH t AS (
            SELECT id, content FROM test_search_duplicate_clause_score
            WHERE content MATCH_ANY 'spider' AND content MATCH_ANY 'spider'
        )
        SELECT id, score() AS bm25_score FROM t
        ORDER BY bm25_score DESC LIMIT 100
    """

    qt_match_duplicate_min_score """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE ((content MATCH_ANY 'spider') OR (content MATCH_ANY 'spider')) AND score() > 1
        ORDER BY bm25_score DESC LIMIT 100
    """
    qt_match_duplicate_warm """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider') OR (content MATCH_ANY 'spider')
        ORDER BY bm25_score DESC LIMIT 100
    """
    qt_unscored_or """
        SELECT id FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider') OR (content MATCH_ANY 'spider')
        ORDER BY id
    """
    qt_unscored_and """
        SELECT id FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider') AND (content MATCH_ANY 'spider')
        ORDER BY id
    """
    qt_search_not """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE search('(content:spider OR content:spider) AND NOT content:web')
        ORDER BY bm25_score DESC LIMIT 100
    """
    qt_match_not """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider' OR content MATCH_ANY 'spider')
          AND NOT (content MATCH_ANY 'web')
        ORDER BY bm25_score DESC LIMIT 100
    """
    qt_match_not_duplicate """
        SELECT id, score() AS bm25_score
        FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider' OR content MATCH_ANY 'spider')
          AND NOT (content MATCH_ANY 'web' OR content MATCH_ANY 'web')
        ORDER BY bm25_score DESC LIMIT 100
    """
    qt_match_unrelated_cte """
        WITH unrelated AS (
            SELECT id FROM test_search_duplicate_clause_score
            WHERE content MATCH_ANY 'web' OR content MATCH_ANY 'web'
        )
        SELECT id, score() AS bm25_score FROM test_search_duplicate_clause_score
        WHERE content MATCH_ANY 'spider' OR content MATCH_ANY 'spider'
        ORDER BY bm25_score DESC LIMIT 100
    """
    qt_unscored_cte_or """
        WITH plain AS (
            SELECT id FROM test_search_duplicate_clause_score
            WHERE content MATCH_ANY 'web' OR content MATCH_ANY 'web'
        )
        SELECT id FROM plain ORDER BY id
    """
    qt_unscored_cte_and """
        WITH plain AS (
            SELECT id FROM test_search_duplicate_clause_score
            WHERE content MATCH_ANY 'web' AND content MATCH_ANY 'web'
        )
        SELECT id FROM plain ORDER BY id
    """
    qt_unscored_not """
        SELECT id FROM test_search_duplicate_clause_score
        WHERE (content MATCH_ANY 'spider' OR content MATCH_ANY 'spider')
          AND NOT (content MATCH_ANY 'web' OR content MATCH_ANY 'web')
        ORDER BY id
    """
    qt_match_derived_set_var_hint """
        SELECT id, score() AS bm25_score FROM (
            SELECT /*+ SET_VAR(enable_inverted_index_wand_query=false) */ id, content
            FROM test_search_duplicate_clause_score
            WHERE content MATCH_ANY 'spider' OR content MATCH_ANY 'spider'
        ) t ORDER BY bm25_score DESC LIMIT 100
    """
    qt_match_derived_preagg_hint """
        SELECT id, score() AS bm25_score FROM (
            SELECT /*+ PREAGGOPEN */ id, content FROM test_search_duplicate_clause_score
            WHERE content MATCH_ANY 'spider' OR content MATCH_ANY 'spider'
        ) t ORDER BY bm25_score DESC LIMIT 100
    """
}
