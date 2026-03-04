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

suite("test_search_default_field_operator", "p0") {
    def tableName = "search_enhanced_test"

    // Pin enable_common_expr_pushdown to prevent CI flakiness from fuzzy testing.
    sql """ set enable_common_expr_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with inverted indexes
    // firstname: with lower_case for case-insensitive wildcard search
    // tags: with parser for tokenized search
    // tags_exact: without parser specification (default behavior) for exact matching
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            firstname VARCHAR(100),
            tags VARCHAR(200),
            tags_exact VARCHAR(200),
            INDEX idx_firstname(firstname) USING INVERTED PROPERTIES("lower_case" = "true"),
            INDEX idx_tags(tags) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_tags_exact(tags_exact) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // Insert test data matching the image requirements
    sql """INSERT INTO ${tableName} VALUES
        (1, 'Chris', 'foo bar', 'foo bar'),
        (2, 'Christopher', 'foobar', 'foobar'),
        (3, 'Kevin', 'bar foo', 'bar foo'),
        (4, 'kevin', 'foolish bark', 'foolish bark')
    """

    // Wait for index building
    Thread.sleep(3000)

    // ============ Test 1: Wildcard Prefix with Default Field ============
    // Requirement: firstname EQ Chris*
    // SQL: search('Chris*', '{"default_field":"firstname"}')
    // Expected: Chris (1), Christopher (2)
    // Note: Without parser, inverted index is case-sensitive
    qt_wildcard_prefix """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('Chris*', '{"default_field":"firstname"}')
        ORDER BY id
    """

    // ============ Test 2: Multi-term AND with Default Operator ============
    // Requirement: tags EQ foo bar (with AND semantics)
    // SQL: search('foo bar', '{"default_field":"tags","default_operator":"and"}')
    // Expected: 'foo bar' (1), 'bar foo' (3)
    qt_multi_term_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, tags
        FROM ${tableName}
        WHERE search('foo bar', '{"default_field":"tags","default_operator":"and"}')
        ORDER BY id
    """

    // ============ Test 3: Multi-term OR with Default Operator ============
    // Requirement: tags EQ foo OR bark (with OR semantics)
    // SQL: search('foo bark', '{"default_field":"tags","default_operator":"or"}')
    // Expected: 'foo bar' (1), 'bar foo' (3), 'foolish bark' (4)
    qt_multi_term_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, tags
        FROM ${tableName}
        WHERE search('foo bark', '{"default_field":"tags","default_operator":"or"}')
        ORDER BY id
    """

    // ============ Test 4: Multi-wildcard AND ============
    // Requirement: tags EQ foo* bar* (with AND semantics)
    // SQL: search('foo* bar*', '{"default_field":"tags","default_operator":"and"}')
    // Expands to: tags:foo* AND tags:bar*
    // Expected: rows with tokens matching foo* AND tokens matching bar*
    // - 'foo bar' (1): tokens=['foo','bar'] - matches foo* ✓ and bar* ✓
    // - 'foobar' (2): tokens=['foobar'] - matches foo* ✓ but NOT bar* ✗ (excluded)
    // - 'bar foo' (3): tokens=['bar','foo'] - matches foo* ✓ and bar* ✓
    // - 'foolish bark' (4): tokens=['foolish','bark'] - matches foo* ✓ and bar* ✓
    qt_wildcard_multi_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, tags
        FROM ${tableName}
        WHERE search('foo* bar*', '{"default_field":"tags","default_operator":"and"}')
        ORDER BY id
    """

    // ============ Test 5: Explicit OR operator overrides default ============
    // SQL: search('foo OR bark', '{"default_field":"tags","default_operator":"and"}')
    // The explicit OR in DSL should override the default 'and' operator
    // Expected: 'foo bar' (1), 'bar foo' (3), 'foolish bark' (4)
    qt_explicit_or_override """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, tags
        FROM ${tableName}
        WHERE search('foo OR bark', '{"default_field":"tags","default_operator":"and"}')
        ORDER BY id
    """

    // ============ Test 6: EXACT function with default field ============
    // Requirement: EXACT(foo bar) on tags_exact field (no tokenization)
    // SQL: search('EXACT(foo bar)', '{"default_field":"tags_exact"}')
    // Expected: 'foo bar' (1) only - exact string match
    qt_exact_function """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, tags_exact
        FROM ${tableName}
        WHERE search('EXACT(foo bar)', '{"default_field":"tags_exact"}')
        ORDER BY id
    """

    // ============ Test 7: Traditional syntax still works ============
    // Ensure backward compatibility - original syntax unchanged
    qt_traditional_syntax """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('firstname:Chris*')
        ORDER BY id
    """

    // ============ Test 8: Single term with default field ============
    qt_single_term """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, tags
        FROM ${tableName}
        WHERE search('bar', '{"default_field":"tags"}')
        ORDER BY id
    """

    // ============ Test 9: Wildcard in middle ============
    qt_wildcard_middle """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('*ris*', '{"default_field":"firstname"}')
        ORDER BY id
    """

    // ============ Test 10: Case sensitivity for wildcard ============
    // Without parser, wildcard queries are case-sensitive (matches Lucene behavior)
    // CHRIS* won't match Chris/Christopher
    qt_case_sensitive """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('CHRIS*', '{"default_field":"firstname"}')
        ORDER BY id
    """

    // ============ Test 11: Default operator is OR when not specified ============
    qt_default_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, tags
        FROM ${tableName}
        WHERE search('foo bark', '{"default_field":"tags"}')
        ORDER BY id
    """

    // ============ Test 12: ANY function with default field ============
    qt_any_function """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, tags
        FROM ${tableName}
        WHERE search('ANY(foo bark)', '{"default_field":"tags"}')
        ORDER BY id
    """

    // ============ Test 13: ALL function with default field ============
    qt_all_function """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, tags
        FROM ${tableName}
        WHERE search('ALL(foo bar)', '{"default_field":"tags"}')
        ORDER BY id
    """

    // ============ Test 14: Complex wildcard pattern ============
    qt_complex_wildcard """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('?evin', '{"default_field":"firstname"}')
        ORDER BY id
    """

    // ============ Test 15: Default field with explicit AND ============
    qt_explicit_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, tags
        FROM ${tableName}
        WHERE search('foo AND bar', '{"default_field":"tags"}')
        ORDER BY id
    """

    // ============ Test 16: Multiple fields still work ============
    qt_multiple_fields """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname, tags
        FROM ${tableName}
        WHERE search('firstname:Chris* OR tags:bark')
        ORDER BY id
    """

    // ============ Test 17: NOT operator with default field ============
    qt_not_operator """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, tags
        FROM ${tableName}
        WHERE search('NOT foobar', '{"default_field":"tags"}')
        ORDER BY id
    """

    // ============ Test 18: Combining different parameter counts ============
    // Tests mixing 1-param and 2-param search() calls in same query
    // - search('firstname:Chris*'): 1-param, traditional syntax → matches id 1,2
    // - search('foo*', '{"default_field":"tags","default_operator":"or"}'): 2-param with JSON options → matches id 1,3,4
    // - OR combination → matches id 1,2,3,4 (all rows)
    qt_param_count_mix """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id
        FROM ${tableName}
        WHERE search('firstname:Chris*') OR search('foo*', '{"default_field":"tags","default_operator":"or"}')
        ORDER BY id
    """

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
}
