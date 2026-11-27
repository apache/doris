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

    // A failed scored child must remain an error even when its sibling adds no scores.
    for (boolean wand : [false, true]) {
        sql "set enable_inverted_index_wand_query = ${wand}"
        for (String sibling : ["tags MATCH_ANY 'missing_token'", "id < 0"]) {
            for (boolean reverseChildren : [false, true]) {
                String incomplete = "(title MATCH_ANY 'apple' AND id > 0)"
                String predicate = reverseChildren ? "${sibling} OR ${incomplete}"
                                                   : "${incomplete} OR ${sibling}"
                // Repeat the query to cover bitmap-cache reuse after the error.
                for (int attempt = 0; attempt < 2; ++attempt) {
                    test {
                        sql """
                            SELECT id, score() AS bm25_score
                            FROM test_search_score_multi_column_mixed
                            WHERE ${predicate}
                            ORDER BY bm25_score DESC LIMIT 100
                        """
                        exception "Cannot score a compound predicate with incomplete index evaluation"
                    }
                }
            }
        }
    }

    // Each INSERT writes its own segment, so some segments store only v.title or only v.content.
    // Keep every path materialized and indexed; the fuzzy defaults may store paths in the sparse
    // or doc-value column, where a segment cannot prove a path absent.
    sql """ set default_variant_enable_doc_mode = false """
    sql """ set default_variant_max_subcolumns_count = 0 """
    sql "DROP TABLE IF EXISTS test_search_score_variant_absent_path"
    sql """
        CREATE TABLE test_search_score_variant_absent_path (
            id INT NOT NULL,
            v VARIANT NULL,
            INDEX idx_v(v) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_search_score_variant_absent_path VALUES
            (1, parse_to_variant('{"title":"apple search guide","content":"search database engine"}')),
            (2, parse_to_variant('{"title":"banana guide","content":"analytics engine"}'))
    """
    sql """
        INSERT INTO test_search_score_variant_absent_path VALUES
            (3, parse_to_variant('{"title":"apple archive"}')),
            (4, parse_to_variant('{"title":"database manual"}'))
    """
    sql """
        INSERT INTO test_search_score_variant_absent_path VALUES
            (5, parse_to_variant('{"content":"apple search"}')),
            (6, parse_to_variant('{"content":"database analytics"}'))
    """
    sql """
        INSERT INTO test_search_score_variant_absent_path VALUES
            (7, parse_to_variant('{"category":"apple search metadata"}')),
            (8, parse_to_variant('{}')),
            (9, parse_to_variant('{"title":"apple database","content":"search analytics"}'))
    """

    order_qt_variant_search """
        SELECT id, ROUND(score(), 4) AS bm25_score
        FROM test_search_score_variant_absent_path
        WHERE search('v.title:apple OR v.content:search')
        ORDER BY score() DESC LIMIT 100
    """

    order_qt_variant_match_or """
        SELECT id, ROUND(score(), 4) AS bm25_score
        FROM test_search_score_variant_absent_path
        WHERE v['title'] MATCH_ANY 'apple' OR v['content'] MATCH_ANY 'search'
        ORDER BY score() DESC LIMIT 100
    """

    // An absent v.content is NULL, so NOT keeps a row only when v.title matches.
    order_qt_variant_not_absent """
        SELECT id, ROUND(score(), 4) AS bm25_score
        FROM test_search_score_variant_absent_path
        WHERE NOT (v['content'] MATCH_ANY 'search') OR v['title'] MATCH_ANY 'apple'
        ORDER BY score() DESC LIMIT 100
    """

    sql "DROP TABLE IF EXISTS test_search_score_nullable_not_and"
    sql """
        CREATE TABLE test_search_score_nullable_not_and (
            id INT NOT NULL,
            title STRING NULL,
            content STRING NULL,
            INDEX idx_title(title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content(content) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default:1")
    """
    sql """
        INSERT INTO test_search_score_nullable_not_and VALUES
            (1, NULL, 'hit'), (2, NULL, 'miss'), (3, 'apple', 'miss')
    """

    // Keep NOT over AND. NULL AND FALSE is FALSE, so NOT keeps row 2 of the nullable table and
    // row 4 of the VARIANT table, which has no v.content and a v.title without 'apple'.
    sql """ set disable_nereids_expression_rules = 'SIMPLIFY_NOT_EXPR' """
    order_qt_nullable_not_and """
        SELECT id FROM test_search_score_nullable_not_and
        WHERE NOT (title MATCH_ANY 'x' AND content MATCH_ANY 'hit')
    """
    order_qt_variant_not_and_absent """
        SELECT id FROM test_search_score_variant_absent_path
        WHERE NOT (v['content'] MATCH_ANY 'search' AND v['title'] MATCH_ANY 'apple')
    """
    sql """ set disable_nereids_expression_rules = '' """

    // Without escaped keys, the segment stores row 1 under a.b.c, and v['a.b'] reads
    // {"c":"hit"} from there.
    sql "DROP TABLE IF EXISTS test_search_score_variant_dotted_path"
    sql """
        CREATE TABLE test_search_score_variant_dotted_path (
            id INT NOT NULL,
            v VARIANT NULL,
            INDEX idx_v(v) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_search_score_variant_dotted_path VALUES
            (1, parse_to_variant('{"a":{"b":{"c":"hit"}}}')),
            (2, parse_to_variant('{"a":{"x":"hit"}}'))
    """
    order_qt_variant_dotted_path """
        SELECT id FROM test_search_score_variant_dotted_path WHERE v['a.b'] MATCH_ANY 'hit'
    """
}
