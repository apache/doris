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

suite("test_search_score_topn_predicates", "p0") {
    sql "DROP TABLE IF EXISTS test_search_score_topn_predicates"

    sql """
        CREATE TABLE test_search_score_topn_predicates (
            id INT,
            status VARCHAR(20),
            plain_status VARCHAR(20),
            category VARCHAR(20),
            title TEXT,
            body TEXT,
            INDEX idx_status (status) USING INVERTED,
            INDEX idx_category (category) USING INVERTED,
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true"),
            INDEX idx_body (body) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """
        INSERT INTO test_search_score_topn_predicates VALUES
            (1, 'drop', 'drop', 'plain', 'apple apple apple apple apple apple apple apple apple apple apple apple', 'alpha'),
            (2, 'keep', 'keep', 'plain', 'apple', 'alpha'),
            (3, 'keep', 'keep', 'plain', 'apple apple apple apple apple', 'alpha'),
            (4, 'keep', 'keep', 'plain', 'apple apple apple', 'beta beta beta'),
            (5, 'drop', 'drop', 'plain', 'banana', 'beta beta beta beta beta beta beta beta beta beta beta beta'),
            (6, 'keep', 'keep', 'plain', 'pear', 'beta beta beta beta beta'),
            (7, 'keep', 'keep', 'special', 'cherry cherry cherry cherry', 'gamma'),
            (8, 'drop', 'drop', 'special', 'cherry cherry cherry cherry cherry cherry cherry cherry cherry cherry', 'gamma')
    """

    sql "sync"
    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    sql "set enable_segment_limit_pushdown = true"
    sql "set enable_inverted_index_query_cache = false"

    qt_single_search """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE search('title:apple')
            ORDER BY s DESC
            LIMIT 1
        ) t
        ORDER BY id
    """

    qt_search_with_equal_predicate """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE search('title:apple') AND status = 'keep'
            ORDER BY s DESC
            LIMIT 1
        ) t
        ORDER BY id
    """

    qt_search_with_plain_equal_predicate """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE search('title:apple') AND plain_status = 'keep'
            ORDER BY s DESC
            LIMIT 1
        ) t
        ORDER BY id
    """

    qt_search_with_equal_limit_two """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE search('title:apple') AND status = 'keep'
            ORDER BY s DESC
            LIMIT 2
        ) t
        ORDER BY id
    """

    qt_search_with_equal_offset """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE search('title:apple') AND status = 'keep'
            ORDER BY s DESC
            LIMIT 1 OFFSET 1
        ) t
        ORDER BY id
    """

    qt_search_with_match_predicate """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE search('title:apple') AND status MATCH 'keep'
            ORDER BY s DESC
            LIMIT 1
        ) t
        ORDER BY id
    """

    qt_search_with_range_predicate """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE search('title:apple') AND id > 1
            ORDER BY s DESC
            LIMIT 1
        ) t
        ORDER BY id
    """

    qt_multiple_search_predicates """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE search('title:apple') AND search('body:beta')
            ORDER BY s DESC
            LIMIT 1
        ) t
        ORDER BY id
    """

    qt_search_with_score_range_only """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE search('title:apple') AND score() > 0
            ORDER BY s DESC
            LIMIT 2
        ) t
        ORDER BY id
    """

    qt_search_with_score_range_and_other_predicate """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE search('title:apple') AND score() > 0 AND status = 'keep'
            ORDER BY s DESC
            LIMIT 1
        ) t
        ORDER BY id
    """

    qt_nested_search_with_other_predicate """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE (search('title:cherry') OR category MATCH 'special') AND status = 'keep'
            ORDER BY s DESC
            LIMIT 1
        ) t
        ORDER BY id
    """

    qt_not_search_with_other_search """
        SELECT id FROM (
            SELECT id, score() AS s
            FROM test_search_score_topn_predicates
            WHERE NOT search('title:apple') AND search('body:beta') AND status = 'keep'
            ORDER BY s DESC
            LIMIT 1
        ) t
        ORDER BY id
    """
}
