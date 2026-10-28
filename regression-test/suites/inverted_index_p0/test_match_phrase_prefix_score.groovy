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

// score() over MATCH_PHRASE_PREFIX: never fails, counts the terms the prefix expands to in the
// idf, and gives the same scores on every inverted index storage format. A prefix that expands to
// fewer documents than the other terms used to make 4.0/4.1 throw "UnionTermIterator does not
// support scoring".
suite("test_match_phrase_prefix_score", "p0") {
    sql "DROP TABLE IF EXISTS test_match_phrase_prefix_score_v2"
    sql "DROP TABLE IF EXISTS test_match_phrase_prefix_score_v3"
    sql "DROP TABLE IF EXISTS test_match_phrase_prefix_score_snii"

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
    sql """
        CREATE TABLE test_match_phrase_prefix_score_snii (
            id INT NULL,
            c1 TEXT NULL,
            INDEX idx_c1 (c1) USING INVERTED PROPERTIES("lower_case" = "true", "parser" = "english", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1", "inverted_index_storage_format" = "SNII");
    """

    // One insert per table keeps every table to a single segment, so the segment statistics that
    // score the expanded terms are the table's.
    def rows = """(1, 'apple red'), (2, 'apple green'), (3, 'banana yellow'), (4, 'apple red green'),
                  (5, 'apple apple red'), (6, 'red red red'), (7, 'apple gray'), (8, 'apple garden path')"""
    sql "INSERT INTO test_match_phrase_prefix_score_v2 VALUES ${rows}"
    sql "INSERT INTO test_match_phrase_prefix_score_v3 VALUES ${rows}"
    sql "INSERT INTO test_match_phrase_prefix_score_snii VALUES ${rows}"
    sql "sync"

    // 'apple r': the reported query. 'apple red' is rarer than 'apple', so the prefix is the
    // shorter list of the phrase.
    order_qt_v2_apple_r """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v2
                            WHERE c1 MATCH_PHRASE_PREFIX 'apple r' ORDER BY score() DESC LIMIT 10 """
    order_qt_v3_apple_r """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v3
                            WHERE c1 MATCH_PHRASE_PREFIX 'apple r' ORDER BY score() DESC LIMIT 10 """
    order_qt_snii_apple_r """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_snii
                              WHERE c1 MATCH_PHRASE_PREFIX 'apple r' ORDER BY score() DESC LIMIT 10 """

    // 'apple gr' expands to green and gray, both rarer than apple.
    order_qt_v2_apple_gr """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v2
                             WHERE c1 MATCH_PHRASE_PREFIX 'apple gr' ORDER BY score() DESC LIMIT 10 """
    order_qt_v3_apple_gr """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v3
                             WHERE c1 MATCH_PHRASE_PREFIX 'apple gr' ORDER BY score() DESC LIMIT 10 """
    order_qt_snii_apple_gr """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_snii
                               WHERE c1 MATCH_PHRASE_PREFIX 'apple gr' ORDER BY score() DESC LIMIT 10 """

    // Two phrase terms before the prefix.
    order_qt_v2_apple_red_g """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v2
                                WHERE c1 MATCH_PHRASE_PREFIX 'apple red g' ORDER BY score() DESC LIMIT 10 """
    order_qt_v3_apple_red_g """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v3
                                WHERE c1 MATCH_PHRASE_PREFIX 'apple red g' ORDER BY score() DESC LIMIT 10 """
    order_qt_snii_apple_red_g """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_snii
                                  WHERE c1 MATCH_PHRASE_PREFIX 'apple red g' ORDER BY score() DESC LIMIT 10 """

    // A prefix alone is a constant score on every format.
    order_qt_v2_prefix_only """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v2
                                WHERE c1 MATCH_PHRASE_PREFIX 'app' ORDER BY score() DESC LIMIT 10 """
    order_qt_v3_prefix_only """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_v3
                                WHERE c1 MATCH_PHRASE_PREFIX 'app' ORDER BY score() DESC LIMIT 10 """
    order_qt_snii_prefix_only """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_snii
                                  WHERE c1 MATCH_PHRASE_PREFIX 'app' ORDER BY score() DESC LIMIT 10 """

    // The same rows without score().
    order_qt_v3_rows """ SELECT id FROM test_match_phrase_prefix_score_v3
                         WHERE c1 MATCH_PHRASE_PREFIX 'apple gr' """

    // A limit of one expansion over two segments. Segment A holds "apple rose", segment B holds
    // "apple red" and "apple rose"; each segment expands "r" to its smallest term, rose in A and
    // red in B. The tablet scores the prefix on red alone, the smallest term of the tablet, and
    // counts red in every segment that holds it. Counting rose from segment A only would give it
    // a document frequency of 1 instead of 2.
    sql "DROP TABLE IF EXISTS test_match_phrase_prefix_score_capped"
    sql """
        CREATE TABLE test_match_phrase_prefix_score_capped (
            id INT NULL,
            c1 TEXT NULL,
            INDEX idx_c1 (c1) USING INVERTED PROPERTIES("lower_case" = "true", "parser" = "english", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1", "inverted_index_storage_format" = "V3");
    """
    sql "INSERT INTO test_match_phrase_prefix_score_capped VALUES (1, 'apple rose')"
    sql "INSERT INTO test_match_phrase_prefix_score_capped VALUES (2, 'apple red'), (3, 'apple rose')"
    sql "sync"
    sql "set inverted_index_max_expansions = 1"
    order_qt_capped_apple_r """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_capped
                                WHERE c1 MATCH_PHRASE_PREFIX 'apple r' ORDER BY score() DESC LIMIT 10 """
    sql "set inverted_index_max_expansions = 50"
    order_qt_uncapped_apple_r """ SELECT id, round(score(), 4) s FROM test_match_phrase_prefix_score_capped
                                  WHERE c1 MATCH_PHRASE_PREFIX 'apple r' ORDER BY score() DESC LIMIT 10 """
}
