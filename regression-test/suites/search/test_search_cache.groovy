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

suite("test_search_cache", "p0") {
    def tableName = "search_cache_test"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(200),
            content VARCHAR(500),
            INDEX idx_title(title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content(content) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """INSERT INTO ${tableName} VALUES
        (1, 'apple banana cherry', 'red fruit sweet'),
        (2, 'banana grape mango', 'yellow fruit tropical'),
        (3, 'cherry plum peach', 'stone fruit summer'),
        (4, 'apple grape kiwi', 'green fruit fresh'),
        (5, 'mango pineapple coconut', 'tropical fruit exotic'),
        (6, 'apple cherry plum', 'mixed fruit salad'),
        (7, 'banana coconut papaya', 'smoothie blend tropical'),
        (8, 'grape cherry apple', 'wine fruit tart')
    """
    sql """sync"""

    // sync ensures data is flushed. Sleep is a best-effort wait for
    // background index availability; may need to increase under load.
    Thread.sleep(2000)

    // Test 1: Cache consistency - same query returns same results with cache enabled
    def result1 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
        id FROM ${tableName}
        WHERE search('title:apple')
        ORDER BY id
    """

    // Run same query again (should hit cache)
    def result2 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
        id FROM ${tableName}
        WHERE search('title:apple')
        ORDER BY id
    """

    // Results must be identical (cache hit returns same data)
    assertEquals(result1, result2)

    // Test 2: Cache disabled returns same results as cache enabled
    def result_no_cache = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=false) */
        id FROM ${tableName}
        WHERE search('title:apple')
        ORDER BY id
    """
    assertEquals(result1, result_no_cache)

    // Test 3: Multi-field query cache consistency
    def mf_result1 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
        id FROM ${tableName}
        WHERE search('title:cherry OR content:tropical')
        ORDER BY id
    """

    def mf_result2 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
        id FROM ${tableName}
        WHERE search('title:cherry OR content:tropical')
        ORDER BY id
    """
    assertEquals(mf_result1, mf_result2)

    // Test 4: Different queries produce different cache entries
    def diff_result = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
        id FROM ${tableName}
        WHERE search('title:banana')
        ORDER BY id
    """
    // banana result should differ from apple result
    assertNotEquals(result1, diff_result)

    // Test 5: AND query - cache vs no-cache consistency
    def and_cached = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
        id, title FROM ${tableName}
        WHERE search('title:apple AND title:cherry')
        ORDER BY id
    """

    def and_uncached = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=false) */
        id, title FROM ${tableName}
        WHERE search('title:apple AND title:cherry')
        ORDER BY id
    """
    assertEquals(and_cached, and_uncached)

    // Test 6: Complex boolean query - cache vs no-cache consistency
    def complex_cached = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=true) */
        id, title FROM ${tableName}
        WHERE search('(title:apple OR title:banana) AND content:fruit')
        ORDER BY id
    """

    def complex_uncached = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true,enable_inverted_index_query_cache=false) */
        id, title FROM ${tableName}
        WHERE search('(title:apple OR title:banana) AND content:fruit')
        ORDER BY id
    """
    assertEquals(complex_cached, complex_uncached)

    sql "DROP TABLE IF EXISTS ${tableName}"
}
