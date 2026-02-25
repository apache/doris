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
 * Tests for escape character handling in search() function.
 *
 * Escape semantics in DSL:
 * - Backslash (\) escapes the next character
 * - Escaped space (\ ) joins terms: "First\ Value" -> single term "First Value"
 * - Escaped parentheses (\( \)) are literal characters, not grouping
 * - Escaped colon (\:) is literal, not field separator
 * - Escaped backslash (\\) is a literal backslash
 *
 * Escape chain in Groovy regression tests:
 * - Groovy string: \\\\ -> SQL string: \\ -> DSL: \ (escape char)
 * - Groovy string: \\\\\\\\ -> SQL string: \\\\ -> DSL: \\ -> literal: \
 */
suite("test_search_escape", "p0") {
    def tableName = "search_escape_test"

    // Pin enable_common_expr_pushdown to prevent CI flakiness from fuzzy testing.
    sql """ set enable_common_expr_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with inverted indexes
    // parser=none: store the entire value as a single term (no tokenization)
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(200),
            content VARCHAR(500),
            INDEX idx_title(title) USING INVERTED PROPERTIES("parser" = "none"),
            INDEX idx_content(content) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // Insert test data
    // With parser=none, these values are stored as-is (single terms)
    // Groovy \\\\ -> SQL \\ -> stored as single backslash \
    sql """INSERT INTO ${tableName} VALUES
        (1, 'First Value', 'first content'),
        (2, 'FirstValue', 'second content'),
        (3, 'hello(world)', 'third content'),
        (4, 'hello world', 'fourth content'),
        (5, 'key:value', 'fifth content'),
        (6, 'path\\\\to\\\\file', 'sixth content'),
        (7, 'apple', 'first fruit'),
        (8, 'banana', 'second fruit')
    """

    // Wait for index building
    Thread.sleep(3000)

    // ============ Test 1: Escaped space - search for "First Value" as single term ============
    // DSL: title:First\ Value -> searches for term "First Value" (with space)
    // Groovy: \\\\ -> SQL: \\ -> DSL: \ (escape)
    // This should match row 1 which has "First Value" stored as single term (parser=none)
    qt_escape_space """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:First\\\\ Value')
        ORDER BY id
    """

    // ============ Test 2: Without escape - space separates terms ============
    // DSL: title:First Value -> "First" and "Value" as separate terms (syntax error without field)
    // This query won't work as expected, showing the difference
    // Using phrase query instead to show the contrast
    qt_phrase_query """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:"First Value"')
        ORDER BY id
    """

    // ============ Test 3: Escaped parentheses ============
    // DSL: title:hello\(world\) -> searches for literal "hello(world)"
    // Groovy: \\\\( -> SQL: \\( -> DSL: \( -> literal: (
    qt_escape_parentheses """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:hello\\\\(world\\\\)')
        ORDER BY id
    """

    // ============ Test 4: Escaped colon ============
    // DSL: title:key\:value -> searches for literal "key:value"
    // Groovy: \\\\: -> SQL: \\: -> DSL: \: -> literal: :
    qt_escape_colon """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:key\\\\:value')
        ORDER BY id
    """

    // ============ Test 5: Escaped backslash ============
    // DSL: title:path\\to\\file -> searches for "path\to\file"
    // Groovy: \\\\\\\\ -> SQL: \\\\ -> DSL: \\ -> literal: \
    // Data stored: path\to\file (Groovy \\\\ -> SQL \\ -> stored \)
    qt_escape_backslash """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:path\\\\\\\\to\\\\\\\\file')
        ORDER BY id
    """

    // ============ Test 6: Uppercase AND operator ============
    qt_uppercase_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content
        FROM ${tableName}
        WHERE search('content:first AND content:fruit')
        ORDER BY id
    """

    // ============ Test 7: Uppercase OR operator ============
    qt_uppercase_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content
        FROM ${tableName}
        WHERE search('content:first OR content:second')
        ORDER BY id
    """

    // ============ Test 8: Uppercase NOT operator ============
    qt_uppercase_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content
        FROM ${tableName}
        WHERE search('content:fruit AND NOT content:first')
        ORDER BY id
    """

    // ============ Test 9: Lowercase 'and' should cause parse error ============
    // Per requirement: Only uppercase AND/OR/NOT are operators
    // Lowercase 'and' is treated as a bare term (no field), causing error
    test {
        sql """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content
            FROM ${tableName}
            WHERE search('content:first and content:fruit')
            ORDER BY id
        """
        exception "No field specified and no default_field configured"
    }

    // ============ Test 10: Lowercase 'or' should cause parse error ============
    // Per requirement: Only uppercase AND/OR/NOT are operators
    // Lowercase 'or' is treated as a bare term (no field), causing error
    test {
        sql """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content
            FROM ${tableName}
            WHERE search('content:first or content:second')
            ORDER BY id
        """
        exception "No field specified and no default_field configured"
    }

    // ============ Test 11: Exclamation NOT operator ============
    qt_exclamation_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content
        FROM ${tableName}
        WHERE search('content:fruit AND !content:first')
        ORDER BY id
    """

    // ============ Test 12: Default field with escaped space ============
    // DSL: First\ Value with default_field=title (JSON options format)
    qt_default_field_escape """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('First\\\\ Value', '{"default_field":"title","default_operator":"and"}')
        ORDER BY id
    """

    // ============ Test 13: Lucene mode with escaped space ============
    qt_lucene_mode_escape """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('First\\\\ Value', '{"default_field":"title","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
}
