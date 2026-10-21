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

// score() is supported in a SELECT that reads one table and has a MATCH or SEARCH predicate in
// its WHERE clause: as one minimum score in WHERE, and in the select list, window functions and
// ORDER BY of a SELECT that does not aggregate. A scored SELECT in a subquery, view, CTE or set
// operation needs ORDER BY score() with LIMIT.
suite("test_score_filter_usage", "p0") {
    def unsupportedSelect = "score() can only be used in a SELECT that reads one table"
    def nestedSelect = "score() in a subquery, view, CTE or set operation requires ORDER BY score() with LIMIT"

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

    // A minimum score without ORDER BY score() LIMIT.
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
    order_qt_select_star_score_range """
        SELECT * FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """
    order_qt_table_alias_score_range """
        SELECT t.id, score() AS s FROM test_score_filter_usage t
        WHERE t.c1 MATCH 'apple' AND score() > 0.00001
    """
    explain {
        sql "SELECT id FROM test_score_filter_usage WHERE c1 MATCH 'apple' AND score() >= 0.5"
        contains "SCORE SORT LIMIT: 0"
        contains "SCORE RANGE: score() >= 0.5"
    }

    // Without a minimum score, the scan scores every row it returns.
    order_qt_score_without_range """
        SELECT id, score() AS s FROM test_score_filter_usage WHERE c1 MATCH 'apple'
    """

    // Ordinary sorts of the scored rows.
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
            ORDER BY s DESC LIMIT 9223372036854775807 OFFSET 9223372036854775807
        """
        exception "limit + offset overflows the long range"
    }

    // Window functions of the same SELECT see the score of each row.
    order_qt_window_same_select """
        SELECT id, score() AS s, row_number() OVER (ORDER BY id) AS rn
        FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """
    order_qt_window_order_by_score """
        SELECT id, row_number() OVER (ORDER BY score() DESC, id) AS rn
        FROM test_score_filter_usage WHERE c1 MATCH 'apple'
    """

    // An aggregating SELECT can only use the minimum score. The scan applies it before
    // COUNT_ON_INDEX counts the remaining rows.
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
    order_qt_group_by_score_range """
        SELECT c2, count(*) FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001 GROUP BY c2
    """
    order_qt_grouping_sets_score_range """
        SELECT c2, count(*) FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001 GROUP BY GROUPING SETS ((c2), ())
    """
    test {
        sql "SELECT c2, max(score()) FROM test_score_filter_usage WHERE c1 MATCH 'apple' GROUP BY c2"
        exception "score() can only be used in the WHERE clause of a SELECT with GROUP BY"
    }

    // The minimum score must be one score() > literal or score() >= literal.
    test {
        sql "SELECT id FROM test_score_filter_usage WHERE c1 MATCH 'apple' AND CAST(score() AS INT) > 0"
        exception "score() predicate in WHERE clause must be in the form"
    }
    test {
        sql "SELECT id FROM test_score_filter_usage WHERE c1 MATCH 'apple' AND score() < 0.9"
        exception "score() predicate in WHERE clause must be in the form"
    }
    test {
        sql "SELECT id FROM test_score_filter_usage WHERE c1 MATCH 'apple' AND score() > 0.1 AND score() < 0.9"
        exception "Only one score() range predicate is supported"
    }
    test {
        sql "SELECT id FROM test_score_filter_usage WHERE c1 MATCH 'apple' AND (score() > 0 OR id = 1)"
        exception "score() predicate must be a top-level AND condition"
    }

    // HAVING and QUALIFY run above the scan, where the score is no longer computed.
    test {
        sql """
            SELECT c2, count(*) FROM test_score_filter_usage WHERE c1 MATCH 'apple' AND score() > 0.00001
            GROUP BY c2 HAVING c2 MATCH 'sweet'
        """
        exception "score(), MATCH and SEARCH cannot be used in HAVING or QUALIFY"
    }
    test {
        sql """
            SELECT id, score() AS s, row_number() OVER (ORDER BY id) AS rn FROM test_score_filter_usage
            WHERE c1 MATCH 'apple' QUALIFY rn = 1 AND s > 0.00001
        """
        exception "score(), MATCH and SEARCH cannot be used in HAVING or QUALIFY"
    }
    test {
        sql """
            SELECT id, score() AS s, c1 MATCH 'red' AS m, row_number() OVER (PARTITION BY c1 ORDER BY id) AS rn
            FROM test_score_filter_usage WHERE c1 MATCH 'apple' QUALIFY m AND rn = 1
        """
        exception "score(), MATCH and SEARCH cannot be used in HAVING or QUALIFY"
    }

    // score() is the relevance for the MATCH and SEARCH predicates of its own SELECT on one table.
    test {
        sql "SELECT id, score() FROM test_score_filter_usage WHERE c2 = 'sweet'"
        exception unsupportedSelect
    }
    test {
        sql """
            SELECT a.id, score() AS s FROM test_score_filter_usage a
            JOIN test_score_filter_usage b ON a.id = b.id WHERE a.c1 MATCH 'apple'
        """
        exception unsupportedSelect
    }
    test {
        sql """
            SELECT a.id FROM test_score_filter_usage a JOIN test_score_filter_usage b
            ON a.id = b.id AND score() > 0.00001 WHERE a.c1 MATCH 'apple'
        """
        exception unsupportedSelect
    }
    test {
        sql """
            SELECT id FROM test_score_filter_usage WHERE c1 MATCH 'apple' AND score() > 0.00001
                AND id IN (SELECT id FROM test_score_filter_usage WHERE c2 = 'sweet')
        """
        exception unsupportedSelect
    }
    test {
        sql """
            SELECT * FROM (
                SELECT id, row_number() OVER (ORDER BY id) AS rn FROM test_score_filter_usage
                WHERE c1 MATCH 'apple'
            ) w WHERE score() > 0.00001
        """
        exception unsupportedSelect
    }
    test {
        sql """
            WITH c AS (SELECT id, c1 FROM test_score_filter_usage WHERE c1 MATCH 'apple')
            SELECT id FROM c WHERE score() > 0.00001
        """
        exception unsupportedSelect
    }

    // Without ORDER BY score() LIMIT, a nested scored SELECT would let the predicates of the outer
    // query into its scan, e.g. an outer MATCH would add to the score.
    test {
        sql """
            SELECT id, s FROM (
                SELECT id, c1, score() AS s FROM test_score_filter_usage
                WHERE c1 MATCH 'apple' AND score() > 0.00001
            ) x WHERE x.c1 MATCH 'red'
        """
        exception nestedSelect
    }
    test {
        sql """
            SELECT id, s FROM (SELECT id, score() AS s FROM test_score_filter_usage WHERE c1 MATCH 'apple') x
            WHERE s > 0.00001
        """
        exception nestedSelect
    }
    test {
        sql """
            WITH c AS (SELECT id, score() AS s FROM test_score_filter_usage WHERE c1 MATCH 'apple')
            SELECT * FROM c
        """
        exception nestedSelect
    }
    test {
        sql """
            SELECT id, score() AS s FROM test_score_filter_usage WHERE c1 MATCH 'apple'
            UNION ALL
            SELECT id, score() AS s FROM test_score_filter_usage WHERE c1 MATCH 'red'
        """
        exception nestedSelect
    }
    // Decorrelation removes ORDER BY score() LIMIT from a correlated subquery.
    test {
        sql """
            SELECT id FROM test_score_filter_usage b
            WHERE (b.c1 MATCH 'red' OR b.c1 MATCH 'green') AND EXISTS (
                SELECT 1 FROM test_score_filter_usage a
                WHERE a.c1 = b.c1 AND a.c1 MATCH 'apple' AND score() > 0.6
                ORDER BY score() DESC LIMIT 1)
        """
        exception "score() cannot be used in a correlated subquery"
    }
    order_qt_nested_score_topn_having """
        SELECT * FROM (
            SELECT id, score() AS s FROM test_score_filter_usage
            WHERE c1 MATCH 'apple' HAVING c2 = 'sweet' ORDER BY s DESC LIMIT 2
        ) x
    """
    order_qt_nested_score_topn """
        SELECT x.id, x.s, b.c2 FROM (
            SELECT id, score() AS s FROM test_score_filter_usage
            WHERE c1 MATCH 'apple' AND score() > 0.00001 ORDER BY s DESC LIMIT 2
        ) x JOIN test_score_filter_usage b ON x.id = b.id
        WHERE x.s > 0.00001
    """
    order_qt_union_score_topn """
        (SELECT id, score() AS s FROM test_score_filter_usage WHERE c1 MATCH 'green' ORDER BY s DESC LIMIT 1)
        UNION ALL
        (SELECT id, score() AS s FROM test_score_filter_usage WHERE c1 MATCH 'red' ORDER BY s DESC LIMIT 1)
    """

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
