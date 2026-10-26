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

suite("test_score_filter_matrix", "p0") {
    sql """ set enable_common_expr_pushdown = true; """
    def predicates = [match: "c1 MATCH 'apple'", search: "search('c1:apple')"]
    for (String format : ["V2", "V3"]) {
        sql "DROP TABLE IF EXISTS test_score_filter_matrix"
        sql """
            CREATE TABLE test_score_filter_matrix (
                id INT, c1 TEXT, c2 TEXT,
                INDEX idx_c1(c1) USING INVERTED PROPERTIES (
                    "parser"="english", "lower_case"="true", "support_phrase"="true")
            ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num"="1", "inverted_index_storage_format"="${format}")
        """
        sql """
            INSERT INTO test_score_filter_matrix VALUES
                (1, 'apple red', 'sweet'), (2, 'apple green', 'sour'), (3, 'banana yellow', 'sweet')
        """
        predicates.each { label, predicate ->
            def tag = "${format}_${label}"
            "order_qt_${tag}_range"("""
                SELECT id, score() AS s FROM test_score_filter_matrix
                WHERE ${predicate} AND score() > 0.00001
            """)
            "order_qt_${tag}_topn_control"("""
                SELECT id, score() AS s FROM test_score_filter_matrix
                WHERE ${predicate} ORDER BY s DESC LIMIT 100
            """)
            def boundary = sql("""
                SELECT score() AS s FROM test_score_filter_matrix
                WHERE ${predicate} ORDER BY s DESC LIMIT 1
            """)[0][0].toString()
            "order_qt_${tag}_strict_boundary"("""
                SELECT id FROM test_score_filter_matrix
                WHERE ${predicate} AND score() > CAST(${boundary} AS FLOAT)
            """)
            "order_qt_${tag}_inclusive_boundary"("""
                SELECT id FROM test_score_filter_matrix
                WHERE ${predicate} AND score() >= CAST(${boundary} AS FLOAT)
            """)
            "order_qt_${tag}_reversed_ge"("""
                SELECT id FROM test_score_filter_matrix
                WHERE ${predicate} AND 0.00001 <= score()
            """)
            "order_qt_${tag}_lossless_cast"("""
                SELECT id FROM test_score_filter_matrix
                WHERE ${predicate} AND CAST(score() AS DOUBLE) > 0.00001
            """)
            "order_qt_${tag}_high_threshold"("""
                SELECT id FROM test_score_filter_matrix
                WHERE ${predicate} AND score() > 1000000
            """)
            "order_qt_${tag}_null_threshold"("""
                SELECT id FROM test_score_filter_matrix
                WHERE ${predicate} AND score() > CAST(NULL AS DOUBLE)
            """)
            "order_qt_${tag}_no_hits"("""
                SELECT id FROM test_score_filter_matrix
                WHERE ${predicate.replace('apple', 'unmatchedtoken')} AND score() > 0
            """)
            for (String type : ["INT", "DECIMAL(10, 6)"]) {
                test {
                    sql "SELECT id FROM test_score_filter_matrix WHERE ${predicate} " +
                            "AND CAST(score() AS ${type}) > 0"
                    exception "score() predicate in WHERE clause must be in the form"
                }
            }
            for (String range : ["score() < 1", "score() <= 1", "score() = 1", "score() * score() > 0", "score() > id",
                    "score() + 1 > 1"]) {
                test {
                    sql "SELECT id FROM test_score_filter_matrix WHERE ${predicate} AND ${range}"
                    exception "score() predicate in WHERE clause must be in the form"
                }
            }
            test {
                sql "SELECT id FROM test_score_filter_matrix WHERE ${predicate} " +
                        "AND score() > 0.1 AND score() < 0.9"
                exception "Only one score() range predicate is supported"
            }
            test {
                sql "SELECT id FROM test_score_filter_matrix WHERE ${predicate} AND (score() > 0 OR id = 1)"
                exception "score() predicate must be a top-level AND condition"
            }
        }

        sql """
            INSERT INTO test_score_filter_matrix VALUES
                (4, NULL, 'missing'), (5, repeat('apple ', 300), 'repeat'), (6, 'apple blue green', 'fresh')
        """
        predicates.each { label, predicate ->
            def tag = "${format}_${label}"
            "order_qt_${tag}_multirowset"("""
                SELECT id, score() AS s FROM test_score_filter_matrix
                WHERE ${predicate} AND score() > 0
            """)
            "order_qt_${tag}_multirowset_topn_control"("""
                SELECT id, score() AS s FROM test_score_filter_matrix
                WHERE ${predicate} ORDER BY s DESC LIMIT 100
            """)
            "order_qt_${tag}_long_norm_threshold"("""
                SELECT id FROM test_score_filter_matrix
                WHERE ${predicate} AND score() > 0.956
            """)
            "order_qt_${tag}_warm_cache"("""
                SELECT /*+ SET_VAR(enable_inverted_index_query_cache=true) */ id
                FROM test_score_filter_matrix WHERE ${predicate}
            """)
            for (boolean cache : [false, true]) {
                for (boolean wand : [false, true]) {
                    "order_qt_${tag}_cache_${cache}_wand_${wand}"("""
                        SELECT /*+ SET_VAR(enable_inverted_index_query_cache=${cache},
                                enable_inverted_index_wand_query=${wand}) */ id, score() AS s
                        FROM test_score_filter_matrix
                        WHERE ${predicate} AND id % 2 = 0 AND score() > 0
                    """)
                }
            }
        }
        sql "DELETE FROM test_score_filter_matrix WHERE id = 1"
        predicates.each { label, predicate ->
            def tag = "${format}_${label}"
            "order_qt_${tag}_after_delete"("""
                SELECT id, score() AS s FROM test_score_filter_matrix
                WHERE ${predicate} AND score() > 0
            """)
            "order_qt_${tag}_after_delete_topn_control"("""
                SELECT id, score() AS s FROM test_score_filter_matrix
                WHERE ${predicate} ORDER BY s DESC LIMIT 100
            """)
        }

        sql "DROP TABLE IF EXISTS test_score_filter_mow"
        sql """
            CREATE TABLE test_score_filter_mow (
                id INT NOT NULL, c1 TEXT,
                INDEX idx_c1(c1) USING INVERTED PROPERTIES (
                    "parser"="english", "lower_case"="true", "support_phrase"="true")
            ) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                "inverted_index_storage_format"="${format}")
        """
        sql "INSERT INTO test_score_filter_mow VALUES (1, 'apple red'), (2, 'apple green'), (3, 'banana yellow')"
        sql "INSERT INTO test_score_filter_mow VALUES (1, 'banana yellow'), (3, 'apple apple'), (4, NULL)"
        predicates.each { label, predicate ->
            def tag = "${format}_${label}"
            "order_qt_${tag}_mow"("""
                SELECT id, score() AS s FROM test_score_filter_mow
                WHERE ${predicate} AND score() > 0
            """)
            "order_qt_${tag}_mow_topn_control"("""
                SELECT id, score() AS s FROM test_score_filter_mow
                WHERE ${predicate} ORDER BY s DESC LIMIT 100
            """)
        }
        sql "DELETE FROM test_score_filter_mow WHERE id = 2"
        predicates.each { label, predicate ->
            def tag = "${format}_${label}"
            "order_qt_${tag}_mow_after_delete"("""
                SELECT id, score() AS s FROM test_score_filter_mow
                WHERE ${predicate} AND score() > 0
            """)
            "order_qt_${tag}_mow_after_delete_topn_control"("""
                SELECT id, score() AS s FROM test_score_filter_mow
                WHERE ${predicate} ORDER BY s DESC LIMIT 100
            """)
        }
    }
    sql "DROP TABLE IF EXISTS test_score_filter_ann"
    sql """
        CREATE TABLE test_score_filter_ann (
            id INT NOT NULL, c1 TEXT, embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_c1(c1) USING INVERTED PROPERTIES("parser"="english"),
            INDEX idx_ann(embedding) USING ANN
                PROPERTIES("index_type"="hnsw", "metric_type"="l2_distance", "dim"="2")
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "inverted_index_storage_format"="V3")
    """
    sql """
        INSERT INTO test_score_filter_ann VALUES
            (1, 'apple red green blue yellow orange pear peach grape lemon', [0.0, 0.0]),
            (2, 'apple apple apple', [1.0, 1.0]), (3, 'banana yellow', [2.0, 2.0])
    """
    qt_ann_control """
        SELECT id, l2_distance_approximate(embedding, [0.0, 0.0]) AS dist
        FROM test_score_filter_ann WHERE c1 MATCH 'apple' ORDER BY dist LIMIT 1
    """
    qt_ann_after_score_filter """
        SELECT id, l2_distance_approximate(embedding, [0.0, 0.0]) AS dist
        FROM test_score_filter_ann WHERE c1 MATCH 'apple' AND score() > 0.5
        ORDER BY dist LIMIT 1
    """
    // The minimum score is a filter of the scan, so the nearest row (id 1) is never chosen by the
    // ANN TopN just to be discarded by the score filter afterwards.
    explain {
        sql """
            SELECT id, l2_distance_approximate(embedding, [0.0, 0.0]) AS dist
            FROM test_score_filter_ann WHERE c1 MATCH 'apple' AND score() > 0.5
            ORDER BY dist LIMIT 1
        """
        contains "ANN SORT LIMIT: 1"
        contains "SCORE SORT LIMIT: 0"
        notContains "SCORE RANGE"
    }

    test {
        sql "SELECT id FROM test_score_filter_matrix WHERE score() > 0"
        exception "score() can only be used in a SELECT that reads one table"
    }
}
