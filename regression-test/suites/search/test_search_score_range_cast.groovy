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

suite("test_search_score_range_cast", "p0") {
    // A score range predicate wrapped in a lossy CAST must keep the original expression
    // semantics: CAST(score() AS DECIMALV3(2,1)) >= 0.5 compares the rounded score, and the
    // raw score threshold used by the storage layer would incorrectly drop rows (DORIS-28583).
    def tableName = "test_search_score_range_cast"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title TEXT,
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """INSERT INTO ${tableName} VALUES
        (1, 'apple'), (2, 'apple apple'), (3, 'apple apple apple'),
        (4, 'apple banana'), (5, 'banana apple orange'), (6, 'banana banana'),
        (7, 'apple kiwi pear mango'), (8, 'kiwi pear')"""
    sql "sync"
    sql "set enable_segment_limit_pushdown = true"

    // Oracle: materialize the score first, then filter on the rounded value.
    def oracle = sql("""
        SELECT id FROM (
            SELECT id, score() AS s FROM ${tableName}
            WHERE title MATCH_ANY 'apple' ORDER BY s DESC LIMIT 100
        ) q
        WHERE CAST(s AS DECIMALV3(2,1)) >= 0.5
        ORDER BY id""").collect { it[0] as int }
    assertEquals([2, 3], oracle)

    def castRows = sql("""
        SELECT id FROM (
            SELECT id, score() AS s FROM ${tableName}
            WHERE title MATCH_ANY 'apple'
              AND CAST(score() AS DECIMALV3(2,1)) >= 0.5
            ORDER BY s DESC LIMIT 100
        ) q ORDER BY id""").collect { it[0] as int }
    assertEquals(oracle, castRows)

    def tryCastRows = sql("""
        SELECT id FROM (
            SELECT id, score() AS s FROM ${tableName}
            WHERE title MATCH_ANY 'apple'
              AND TRY_CAST(score() AS DECIMALV3(2,1)) >= 0.5
            ORDER BY s DESC LIMIT 100
        ) q ORDER BY id""").collect { it[0] as int }
    assertEquals(oracle, tryCastRows)

    // A threshold above every rounded score still filters everything out.
    assertEquals([], sql("""
        SELECT id FROM (
            SELECT id, score() AS s FROM ${tableName}
            WHERE title MATCH_ANY 'apple'
              AND CAST(score() AS DECIMALV3(2,1)) >= 0.6
            ORDER BY s DESC LIMIT 100
        ) q ORDER BY id""").collect { it[0] as int })

    // The plain (lossless) score range push down keeps working.
    assertEquals([2, 3], sql("""
        SELECT id FROM (
            SELECT id, score() AS s FROM ${tableName}
            WHERE title MATCH_ANY 'apple' AND score() >= 0.45
            ORDER BY s DESC LIMIT 100
        ) q ORDER BY id""").collect { it[0] as int })
}
