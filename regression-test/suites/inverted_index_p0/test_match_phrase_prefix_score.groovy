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

// score() over MATCH_PHRASE_PREFIX must not fail. When the terms the prefix expands to were rarer
// than the other terms of the phrase, the union of them led the phrase and the scorer read its
// norm, which threw "UnionTermIterator does not support scoring".
suite("test_match_phrase_prefix_score", "p0") {
    sql "DROP TABLE IF EXISTS test_match_phrase_prefix_score_v2"
    sql "DROP TABLE IF EXISTS test_match_phrase_prefix_score_v3"

    sql """
        CREATE TABLE test_match_phrase_prefix_score_v2 (
            id INT NULL,
            c1 TEXT NULL,
            INDEX idx_c1 (c1) USING INVERTED PROPERTIES("lower_case" = "true", "parser" = "english", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1", "inverted_index_storage_format" = "V2");
    """
    sql """
        CREATE TABLE test_match_phrase_prefix_score_v3 (
            id INT NULL,
            c1 TEXT NULL,
            INDEX idx_c1 (c1) USING INVERTED PROPERTIES("lower_case" = "true", "parser" = "english", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1", "inverted_index_storage_format" = "V3");
    """

    // One insert per table keeps every table to a single segment.
    def rows = """(1, 'apple red'), (2, 'apple green'), (3, 'banana yellow'), (4, 'apple red green'),
                  (5, 'apple apple red'), (6, 'red red red'), (7, 'apple gray'), (8, 'apple garden path')"""
    sql "INSERT INTO test_match_phrase_prefix_score_v2 VALUES ${rows}"
    sql "INSERT INTO test_match_phrase_prefix_score_v3 VALUES ${rows}"
    sql "sync"

    // 'apple r': the reported query, 'red' is rarer than 'apple'.
    order_qt_v2_apple_r """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v2
                            WHERE c1 MATCH_PHRASE_PREFIX 'apple r' ORDER BY score() DESC LIMIT 10 """
    order_qt_v3_apple_r """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v3
                            WHERE c1 MATCH_PHRASE_PREFIX 'apple r' ORDER BY score() DESC LIMIT 10 """

    // 'apple gr' expands to green and gray, both rarer than apple.
    order_qt_v2_apple_gr """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v2
                             WHERE c1 MATCH_PHRASE_PREFIX 'apple gr' ORDER BY score() DESC LIMIT 10 """
    order_qt_v3_apple_gr """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v3
                             WHERE c1 MATCH_PHRASE_PREFIX 'apple gr' ORDER BY score() DESC LIMIT 10 """

    // Two phrase terms before the prefix.
    order_qt_v2_apple_red_g """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v2
                                WHERE c1 MATCH_PHRASE_PREFIX 'apple red g' ORDER BY score() DESC LIMIT 10 """
    order_qt_v3_apple_red_g """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v3
                                WHERE c1 MATCH_PHRASE_PREFIX 'apple red g' ORDER BY score() DESC LIMIT 10 """

    // The same rows without score().
    order_qt_v3_rows """ SELECT id FROM test_match_phrase_prefix_score_v3
                         WHERE c1 MATCH_PHRASE_PREFIX 'apple gr' """
}
