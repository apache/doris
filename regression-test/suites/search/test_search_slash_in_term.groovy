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
 * DORIS-24624: Tests for slash (/) character handling in search() function.
 *
 * The slash character is used as a regex delimiter in Lucene query_string syntax
 * (e.g., /pattern/). However, when it appears in the middle of a term (e.g., AC/DC),
 * it should be treated as a regular character, not as a regex delimiter.
 *
 * This test verifies that:
 * 1. Slash within a term (AC/DC) is parsed correctly as a single term
 * 2. Escaped slash (AC\/DC) produces the same result
 * 3. Regex patterns (/pattern/) still work correctly
 * 4. Both standard and lucene modes handle slashes consistently
 */
suite("test_search_slash_in_term", "p0") {
    def tableName = "search_slash_in_term_test"

    sql """ set enable_common_expr_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(200),
            content VARCHAR(500),
            INDEX idx_title(title) USING INVERTED PROPERTIES("parser" = "standard"),
            INDEX idx_content(content) USING INVERTED PROPERTIES("parser" = "standard")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    sql """INSERT INTO ${tableName} VALUES
        (1, 'AC/DC is a rock band', 'rock music'),
        (2, 'AC power supply', 'electrical engineering'),
        (3, 'DC comics', 'entertainment'),
        (4, 'path/to/file', 'file system'),
        (5, 'a/b/c/d', 'multi slash path'),
        (6, 'hello world', 'greeting'),
        (7, 'acdc together', 'no slash')
    """

    // Wait for index building
    Thread.sleep(3000)

    // ============ Test 1: Slash in term with field prefix ============
    // title:AC/DC should parse as single term, standard analyzer tokenizes to "ac" and "dc"
    // With default OR operator, matches rows containing "ac" or "dc" in title
    order_qt_slash_in_term """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:AC/DC')
        ORDER BY id
    """

    // ============ Test 2: Escaped slash should produce same result ============
    // title:AC\/DC should produce the same result as title:AC/DC
    // Groovy: \\\\/ -> SQL: \\/ -> DSL: \/ -> unescaped: /
    order_qt_escaped_slash_in_term """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:AC\\\\/DC')
        ORDER BY id
    """

    // ============ Test 3: Slash in term with default_field (lucene mode) ============
    // Bare AC/DC with default_field should work
    order_qt_slash_bare_lucene """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('AC/DC', '{"default_field":"title","default_operator":"OR","minimum_should_match":0}')
        ORDER BY id
    """

    // ============ Test 4: Escaped slash with default_field should match ============
    order_qt_escaped_slash_bare_lucene """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('AC\\\\/DC', '{"default_field":"title","default_operator":"OR","minimum_should_match":0}')
        ORDER BY id
    """

    // ============ Test 5: Multiple slashes in term ============
    order_qt_multi_slash """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:path/to/file')
        ORDER BY id
    """

    // ============ Test 6: Regex pattern still works ============
    // /[a-z]+/ should be parsed as regex, not as term with slashes
    order_qt_regex_still_works """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:/rock/')
        ORDER BY id
    """

    // ============ Test 7: Slash in term with standard mode ============
    order_qt_slash_standard_mode """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('AC/DC', '{"default_field":"title","mode":"standard"}')
        ORDER BY id
    """
}
