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

// An equality between two columns does not let a MATCH on one column be copied to the other:
// MATCH depends on the inverted index of its column, and a copy would add to score().
suite("infer_match_predicate") {
    sql """ set enable_common_expr_pushdown = true; """
    // c2 has an index without a parser, so MATCH on c2 only matches a whole value.
    sql "DROP TABLE IF EXISTS infer_match_predicate_parser"
    sql """
        CREATE TABLE infer_match_predicate_parser (
            id INT,
            c1 TEXT,
            c2 TEXT,
            INDEX idx_c1 (c1) USING INVERTED PROPERTIES ("parser" = "english"),
            INDEX idx_c2 (c2) USING INVERTED
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO infer_match_predicate_parser VALUES (1, 'apple red', 'apple red'), (2, 'banana', 'banana')"
    order_qt_different_index """
        SELECT id FROM infer_match_predicate_parser
        WHERE c1 = c2 AND (c1 MATCH_ANY 'apple' OR c1 MATCH_ANY 'pear')
    """
    order_qt_wrapped_match """
        SELECT id FROM infer_match_predicate_parser
        WHERE c1 = c2 AND CAST((c1 MATCH_ANY 'apple' OR c1 MATCH_ANY 'pear') AS INT) > 0
    """
    order_qt_search """
        SELECT id FROM infer_match_predicate_parser
        WHERE c1 = c2 AND (search('c1:apple') OR c1 = 'pear')
    """
    order_qt_multi_match """
        SELECT id FROM infer_match_predicate_parser
        WHERE c1 = c2 AND (multi_match(c1, 'any', 'apple') OR multi_match(c1, 'any', 'pear'))
    """

    sql "DROP TABLE IF EXISTS infer_match_predicate_score"
    sql """
        CREATE TABLE infer_match_predicate_score (
            id INT,
            c1 TEXT,
            c2 TEXT,
            INDEX idx_c1 (c1) USING INVERTED PROPERTIES ("parser" = "english"),
            INDEX idx_c2 (c2) USING INVERTED PROPERTIES ("parser" = "english")
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO infer_match_predicate_score VALUES
            (1, 'apple red', 'apple red'), (2, 'apple green', 'apple green'), (3, 'banana yellow', 'banana yellow')
    """
    order_qt_score """
        SELECT id, score() AS s FROM infer_match_predicate_score
        WHERE c1 = c2 AND (c1 MATCH_ANY 'apple' OR c1 MATCH_ANY 'pear') ORDER BY s DESC LIMIT 10
    """
    order_qt_minimum_score """
        SELECT id, score() AS s FROM infer_match_predicate_score
        WHERE c1 = c2 AND (c1 MATCH_ANY 'apple' OR c1 MATCH_ANY 'pear') AND score() > 0.6
        ORDER BY s DESC LIMIT 10
    """
}
