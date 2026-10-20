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

suite("test_score_filter_usage", "p0") {
    sql "DROP TABLE IF EXISTS test_score_filter_usage"
    sql """
        CREATE TABLE test_score_filter_usage (
            id INT,
            c1 TEXT,
            c2 TEXT,
            INDEX idx_c1 (c1) USING INVERTED PROPERTIES (
                "lower_case" = "true", "parser" = "english", "support_phrase" = "true")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "inverted_index_storage_format" = "V2")
    """
    sql """
        INSERT INTO test_score_filter_usage VALUES
            (1, 'apple red', 'sweet'),
            (2, 'apple green', 'sour'),
            (3, 'banana yellow', 'sweet')
    """

    order_qt_where_only_score """
        SELECT id FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """

    order_qt_projected_score_range """
        SELECT id, score() AS s FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """

    order_qt_repeated_score_aliases """
        SELECT id, score() AS first_score, score() AS second_score
        FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """

    qt_ordinary_order_with_offset """
        SELECT id, c2, score() AS s FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
        ORDER BY id LIMIT 1 OFFSET 1
    """

    qt_multiple_order_keys """
        SELECT id, c2, score() AS s FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
        ORDER BY s DESC, id DESC LIMIT 1 OFFSET 1
    """

    qt_reverse_key_order """
        SELECT id, c2, score() AS s FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
        ORDER BY id DESC LIMIT 1
    """

    order_qt_score_sort_without_limit """
        SELECT id, c2, score() AS s FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
        ORDER BY s DESC
    """

    test {
        sql """
            SELECT id, score() AS s FROM test_score_filter_usage
            WHERE c1 MATCH 'apple' AND score() > 0.00001
            ORDER BY id LIMIT 9223372036854775807 OFFSET 9223372036854775807
        """
        exception "limit + offset overflows the long range"
    }

    test {
        sql """
            SELECT id FROM test_score_filter_usage
            WHERE c1 MATCH 'apple' AND CAST(score() AS INT) > 0
        """
        exception "score() predicate in WHERE clause must be in the form"
    }

    // The scan applies the score range before COUNT_ON_INDEX counts the remaining rows.
    qt_count_score_range """
        SELECT /*+ SET_VAR(enable_count_on_index_pushdown=true) */ count(*) FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """
    qt_count_score_range_without_pushdown """
        SELECT /*+ SET_VAR(enable_count_on_index_pushdown=false) */ count(*) FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """
    qt_count_high_score_range """
        SELECT /*+ SET_VAR(enable_count_on_index_pushdown=true) */ count(*) FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 1000000
    """

    order_qt_select_star_score_range """
        SELECT * FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """

    explain {
        sql "SELECT id FROM test_score_filter_usage WHERE c1 MATCH 'apple' AND score() >= 0.5"
        contains "SCORE SORT LIMIT: 0"
        contains "SCORE RANGE: score() >= 0.5"
    }

    // A semi or anti join produces only the outer rows, so the score filter stays on the outer scan.
    order_qt_in_subquery_score_range """
        SELECT id FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
            AND id IN (SELECT id FROM test_score_filter_usage WHERE c2 = 'sweet')
    """
    order_qt_not_exists_score_range """
        SELECT id FROM test_score_filter_usage t
        WHERE c1 MATCH 'apple' AND score() > 0.00001
            AND NOT EXISTS (SELECT 1 FROM test_score_filter_usage s WHERE s.id = t.id AND s.c2 = 'sweet')
    """

    // A left join returns every row of its left side, so the score filter applies to that side.
    order_qt_left_join_score_range """
        SELECT a.id, b.c2 FROM test_score_filter_usage a
        LEFT JOIN test_score_filter_usage b ON a.id = b.id + 100
        WHERE a.c1 MATCH 'apple' AND score() > 0.00001
    """

    // With an inner join score() cannot tell which table it belongs to; filter it in a subquery.
    order_qt_join_scored_subquery """
        SELECT a.id, b.c2 FROM (
            SELECT id FROM test_score_filter_usage WHERE c1 MATCH 'apple' AND score() > 0.00001
        ) a JOIN test_score_filter_usage b ON a.id = b.id
    """
    test {
        sql """
            SELECT a.id FROM test_score_filter_usage a JOIN test_score_filter_usage b ON a.id = b.id
            WHERE a.c1 MATCH 'apple' AND score() > 0.00001
        """
        exception "score() filter must be in the WHERE clause of a query on one table"
    }
    test {
        sql """
            SELECT a.id FROM test_score_filter_usage a JOIN test_score_filter_usage b
            ON a.id = b.id AND score() > 0.00001 WHERE a.c1 MATCH 'apple'
        """
        exception "can not contains Score expression"
    }

    // Each UNION ALL branch filters by the score of its own scan.
    order_qt_union_all_score_range """
        SELECT id FROM (
            SELECT id, c1 FROM test_score_filter_usage
            UNION ALL
            SELECT id, c1 FROM test_score_filter_usage WHERE c2 = 'sweet'
        ) u WHERE c1 MATCH 'apple' AND score() > 0.00001
    """
    // Below a window, the filter would change row_number(); it must be rejected instead.
    test {
        sql """
            SELECT * FROM (
                SELECT id, row_number() OVER (ORDER BY id) AS rn FROM test_score_filter_usage
                WHERE c1 MATCH 'apple'
            ) w WHERE score() > 0.00001
        """
        exception "score() filter must be in the WHERE clause of a query on one table"
    }

    // A projected MATCH must not add its BM25 score to the one the filter compares (0.4700036).
    order_qt_projected_match_above_score """
        SELECT id, c1 MATCH_PHRASE 'apple' AS flag FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.6
    """
    order_qt_projected_match_score """
        SELECT id, score() AS s, c1 MATCH_PHRASE 'apple' AS flag FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """

    // A common sub-expression virtual column must not keep score() away from its binding.
    order_qt_cse_score_expressions """
        SELECT /*+ SET_VAR(experimental_enable_virtual_slot_for_cse=true) */ id,
            sin(abs(score()) + 1) AS a, cos(abs(score()) + 1) AS b
        FROM test_score_filter_usage WHERE c1 MATCH 'apple' AND score() > 0.00001
    """
}
