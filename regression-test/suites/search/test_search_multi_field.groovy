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

/**
 * Tests for multi-field search support in search() function.
 *
 * The 'fields' parameter allows searching across multiple fields with a single query term.
 * This is similar to Elasticsearch's query_string 'fields' parameter.
 *
 * Example:
 *   search('hello', '{"fields":["title","content"]}')
 *   -> Equivalent to: (title:hello OR content:hello)
 *
 *   search('hello world', '{"fields":["title","content"],"default_operator":"and"}')
 *   -> Equivalent to: (title:hello OR content:hello) AND (title:world OR content:world)
 *
 * Multi-field search can also be combined with Lucene mode for MUST/SHOULD/MUST_NOT semantics.
 */
suite("test_search_multi_field") {
    def tableName = "search_multi_field_test"

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with inverted indexes on multiple fields
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(200),
            content VARCHAR(500),
            tags VARCHAR(100),
            category VARCHAR(50),
            INDEX idx_title(title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content(content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_tags(tags) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category(category) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // Insert test data
    sql """INSERT INTO ${tableName} VALUES
        (1, 'machine learning basics', 'introduction to AI and ML', 'ml ai tutorial', 'tech'),
        (2, 'cooking recipes', 'how to make pasta', 'food cooking', 'lifestyle'),
        (3, 'AI in healthcare', 'artificial intelligence applications', 'health ai', 'tech'),
        (4, 'machine maintenance', 'keeping machines running', 'industrial', 'engineering'),
        (5, 'learning guitar', 'music lessons for beginners', 'music learning', 'entertainment'),
        (6, 'deep learning neural networks', 'advanced AI concepts', 'ai ml deep', 'tech'),
        (7, 'car maintenance guide', 'vehicle repair tips', 'auto maintenance', 'automotive'),
        (8, 'cooking machine reviews', 'kitchen appliance ratings', 'cooking appliances', 'lifestyle')
    """

    // Wait for index building
    Thread.sleep(5000)

    // ============ Test 1: Single term across multiple fields ============
    // "machine" in title OR content
    qt_multi_field_single_term """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine', '{"fields":["title","content"]}')
        ORDER BY id
    """

    // ============ Test 2: Multiple terms with AND ============
    // "machine" AND "learning" across title,content
    qt_multi_field_multi_term_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine learning', '{"fields":["title","content"],"default_operator":"and"}')
        ORDER BY id
    """

    // ============ Test 3: Multiple terms with OR (default) ============
    qt_multi_field_multi_term_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine learning', '{"fields":["title","content"],"default_operator":"or"}')
        ORDER BY id
    """

    // ============ Test 4: Explicit AND operator in DSL ============
    qt_multi_field_explicit_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine AND learning', '{"fields":["title","content"]}')
        ORDER BY id
    """

    // ============ Test 5: Mixed - some terms with explicit field ============
    qt_multi_field_mixed """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, category
        FROM ${tableName}
        WHERE search('machine AND category:tech', '{"fields":["title","content"]}')
        ORDER BY id
    """

    // ============ Test 6: Three fields ============
    qt_three_fields """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('ai', '{"fields":["title","content","tags"]}')
        ORDER BY id
    """

    // ============ Test 7: Wildcard across fields ============
    qt_multi_field_wildcard """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('learn*', '{"fields":["title","content","tags"]}')
        ORDER BY id
    """

    // ============ Test 8: NOT operator ============
    qt_multi_field_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine AND NOT cooking', '{"fields":["title","content"]}')
        ORDER BY id
    """

    // ============ Test 9: Complex boolean ============
    qt_multi_field_complex """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('(machine OR ai) AND NOT cooking', '{"fields":["title","content"]}')
        ORDER BY id
    """

    // ============ Test 10: Single field in array (backward compatible) ============
    qt_single_field_array """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine', '{"fields":["title"]}')
        ORDER BY id
    """

    // ============ Test 11: Multi-field with Lucene mode - simple AND ============
    qt_multi_field_lucene_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine AND learning', '{"fields":["title","content"],"mode":"lucene","minimum_should_match":0}')
        ORDER BY id
    """

    // ============ Test 12: Multi-field with Lucene mode - OR ============
    qt_multi_field_lucene_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine OR cooking', '{"fields":["title","content"],"mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 13: Multi-field with Lucene mode - AND OR mixed ============
    // With minimum_should_match=0, SHOULD clauses are discarded when MUST exists
    qt_multi_field_lucene_and_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine AND learning OR cooking', '{"fields":["title","content"],"mode":"lucene","minimum_should_match":0}')
        ORDER BY id
    """

    // ============ Test 14: Multi-field with Lucene mode - minimum_should_match=1 ============
    qt_multi_field_lucene_min_should_1 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine AND learning OR cooking', '{"fields":["title","content"],"mode":"lucene","minimum_should_match":1}')
        ORDER BY id
    """

    // ============ Test 15: Multi-field with Lucene mode - AND NOT ============
    qt_multi_field_lucene_and_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine AND NOT maintenance', '{"fields":["title","content"],"mode":"lucene","minimum_should_match":0}')
        ORDER BY id
    """

    // ============ Test 16: Comparison - same query with default_field vs fields ============
    // Using default_field (single field)
    qt_compare_default_field """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine', '{"default_field":"title"}')
        ORDER BY id
    """

    // Using fields array with single field (should be same as default_field)
    qt_compare_fields_single """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('machine', '{"fields":["title"]}')
        ORDER BY id
    """

    // ============ Test 17: EXACT function across fields ============
    qt_multi_field_exact """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('EXACT(machine learning)', '{"fields":["title","content"]}')
        ORDER BY id
    """

    // ============ Test 18: ANY function across fields ============
    qt_multi_field_any """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('ANY(machine cooking)', '{"fields":["title","content"]}')
        ORDER BY id
    """

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
}
