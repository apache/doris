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

suite("test_match_projection_virtual_column") {
    // This test verifies that MATCH expressions used as projections
    // (not filters) are pushed down as virtual columns on OlapScan
    // and evaluated via inverted index. This is important for queries
    // like FULL OUTER JOIN where MATCH cannot be pushed as a filter.

    def tableA = "test_match_proj_a"
    def tableB = "test_match_proj_b"

    sql "DROP TABLE IF EXISTS ${tableA}"
    sql "DROP TABLE IF EXISTS ${tableB}"

    sql """
        CREATE TABLE ${tableA} (
            k1 INT,
            content TEXT,
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        CREATE TABLE ${tableB} (
            k1 INT,
            val VARCHAR(100)
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """ INSERT INTO ${tableA} VALUES
        (1, 'hello world'),
        (2, 'foo bar baz'),
        (3, 'hello doris database'),
        (4, 'nothing here'),
        (5, 'test hello data')
    """

    sql """ INSERT INTO ${tableB} VALUES
        (1, 'b1'),
        (2, 'b2'),
        (6, 'b6')
    """

    // Test 1: MATCH as projection without join
    order_qt_match_proj_simple """
        SELECT k1, content MATCH_ANY 'hello' as m FROM ${tableA} ORDER BY k1
    """

    // Test 2: MATCH as projection with FULL OUTER JOIN
    // MATCH cannot be pushed as filter in FULL OUTER JOIN,
    // so it should be pushed as a virtual column projection.
    order_qt_match_proj_full_join """
        SELECT ${tableA}.k1, ${tableA}.content MATCH_ANY 'hello' as m
        FROM ${tableA} FULL OUTER JOIN ${tableB} ON ${tableA}.k1 = ${tableB}.k1
        ORDER BY ${tableA}.k1
    """

    // Test 3: Multiple MATCH projections
    order_qt_match_proj_multiple """
        SELECT ${tableA}.k1,
               ${tableA}.content MATCH_ANY 'hello' as m1,
               ${tableA}.content MATCH_ANY 'foo' as m2
        FROM ${tableA} FULL OUTER JOIN ${tableB} ON ${tableA}.k1 = ${tableB}.k1
        ORDER BY ${tableA}.k1
    """

    // Test 4: MATCH projection with additional filter
    order_qt_match_proj_with_filter """
        SELECT ${tableA}.k1, ${tableA}.content MATCH_ANY 'hello' as m
        FROM ${tableA} FULL OUTER JOIN ${tableB} ON ${tableA}.k1 = ${tableB}.k1
        WHERE ${tableA}.k1 > 2
        ORDER BY ${tableA}.k1
    """

    // Test 5: MATCH_PHRASE as projection
    order_qt_match_phrase_proj """
        SELECT k1, content MATCH_PHRASE 'hello world' as m FROM ${tableA} ORDER BY k1
    """

    // Test 6: Verify MATCH as filter still works correctly (regression check)
    order_qt_match_filter_still_works """
        SELECT * FROM ${tableA} WHERE content MATCH_ANY 'hello' ORDER BY k1
    """

    // Test 7: MATCH in INNER JOIN (can be pushed as filter, should still work)
    order_qt_match_inner_join """
        SELECT ${tableA}.k1, ${tableA}.content
        FROM ${tableA} INNER JOIN ${tableB} ON ${tableA}.k1 = ${tableB}.k1
        WHERE ${tableA}.content MATCH_ANY 'hello'
        ORDER BY ${tableA}.k1
    """

    // Test 8: Verify EXPLAIN shows virtual column for FULL OUTER JOIN MATCH projection
    def explainResult = sql """
        EXPLAIN VERBOSE SELECT ${tableA}.k1, ${tableA}.content MATCH_ANY 'hello' as m
        FROM ${tableA} FULL OUTER JOIN ${tableB} ON ${tableA}.k1 = ${tableB}.k1
    """
    def explainStr = explainResult.collect { it.toString() }.join("\n")
    assertTrue(explainStr.contains("MATCH_ANY"), "EXPLAIN should contain MATCH_ANY in virtual column")
    assertTrue(explainStr.contains("virtualColumn="), "EXPLAIN should show virtualColumn for MATCH projection")

    sql "DROP TABLE IF EXISTS ${tableA}"
    sql "DROP TABLE IF EXISTS ${tableB}"
}
