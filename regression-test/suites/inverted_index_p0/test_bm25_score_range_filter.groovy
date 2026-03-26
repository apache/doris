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

suite("test_bm25_score_range_filter", "p0") {
    def tableName = "test_bm25_score_range_filter"

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `content` text NULL,
            INDEX content_idx (`content`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES (1, 'apple banana cherry'); """
    sql """ INSERT INTO ${tableName} VALUES (2, 'apple apple banana'); """
    sql """ INSERT INTO ${tableName} VALUES (3, 'apple apple apple'); """
    sql """ INSERT INTO ${tableName} VALUES (4, 'banana cherry date'); """
    sql """ INSERT INTO ${tableName} VALUES (5, 'cherry date elderberry'); """
    sql """ INSERT INTO ${tableName} VALUES (6, 'apple'); """
    sql """ INSERT INTO ${tableName} VALUES (7, 'apple banana'); """
    sql """ INSERT INTO ${tableName} VALUES (8, 'apple apple banana cherry'); """

    sql "sync"
    sql """ set enable_common_expr_pushdown = true; """

    // Test 1: Basic score range filter with GT (>)
    qt_score_gt """
        SELECT id, content, score() as s
        FROM ${tableName}
        WHERE content match_any 'apple' AND score() > 0.5
        ORDER BY s DESC
        LIMIT 10;
    """

    // Test 2: Score range filter with GE (>=)
    qt_score_ge """
        SELECT id, content, score() as s
        FROM ${tableName}
        WHERE content match_any 'apple' AND score() >= 0.5
        ORDER BY s DESC
        LIMIT 10;
    """

    // Test 3: Score range filter with zero threshold
    qt_score_gt_zero """
        SELECT id, content, score() as s
        FROM ${tableName}
        WHERE content match_any 'apple' AND score() > 0
        ORDER BY s DESC
        LIMIT 10;
    """

    // Test 4: Score range filter with ASC order
    qt_score_asc """
        SELECT id, content, score() as s
        FROM ${tableName}
        WHERE content match_any 'apple' AND score() > 0.3
        ORDER BY s ASC
        LIMIT 5;
    """

    // Test 5: Score range filter with high threshold (may filter all)
    qt_score_high_threshold """
        SELECT id, content, score() as s
        FROM ${tableName}
        WHERE content match_any 'apple' AND score() > 10.0
        ORDER BY s DESC
        LIMIT 10;
    """

    // Test 6: Reversed predicate form (literal < score())
    qt_score_reversed """
        SELECT id, content, score() as s
        FROM ${tableName}
        WHERE content match_any 'apple' AND 0.5 < score()
        ORDER BY s DESC
        LIMIT 10;
    """

    // Test 7: Score range with match_all
    qt_score_match_all """
        SELECT id, content, score() as s
        FROM ${tableName}
        WHERE content match_all 'apple banana' AND score() > 0.3
        ORDER BY s DESC
        LIMIT 10;
    """

    // Test 8: Verify explain shows score range push down
    def explain_result = sql """
        EXPLAIN VERBOSE
        SELECT id, content, score() as s
        FROM ${tableName}
        WHERE content match_any 'apple' AND score() > 0.5
        ORDER BY s DESC
        LIMIT 10;
    """
    log.info("Explain result: ${explain_result}")

    // Test 9: Error case - multiple score predicates not supported
    test {
        sql """
            SELECT id, content, score() as s
            FROM ${tableName}
            WHERE content match_any 'apple' AND score() > 0.3 AND score() < 0.9
            ORDER BY s DESC
            LIMIT 10;
        """
        exception "Only one score() range predicate is supported"
    }

    // Test 10: Error case - score() < X not supported (max_score semantics)
    test {
        sql """
            SELECT id, content, score() as s
            FROM ${tableName}
            WHERE content match_any 'apple' AND score() < 0.9
            ORDER BY s DESC
            LIMIT 10;
        """
        exception "score() predicate in WHERE clause must be in the form of 'score() > literal'"
    }

    // Test 11: Error case - score() compared with string type not supported
    test {
        sql """
            SELECT id, content, score() as s
            FROM ${tableName}
            WHERE content match_any 'apple' AND score() > 'invalid_string'
            ORDER BY s DESC
            LIMIT 10;
        """
        exception "score() function can only be compared with numeric types"
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
}
