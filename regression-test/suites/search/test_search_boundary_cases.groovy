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

suite("test_search_boundary_cases", "p0") {
    def tableName = "search_boundary_test"

    // Pin enable_common_expr_pushdown to prevent CI flakiness from fuzzy testing.
    sql """ set enable_common_expr_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create test table for boundary and edge cases
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            field1 VARCHAR(1000),
            field2 TEXT,
            field3 VARCHAR(500),
            field4 TEXT,
            field5 VARCHAR(200),
            INDEX idx_field1 (field1) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_field2 (field2) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_field3 (field3) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_field4 (field4) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_field5 (field5) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Insert boundary test data
    sql """
        INSERT INTO ${tableName} VALUES
        -- NULL in different combinations
        (1, NULL, NULL, NULL, NULL, NULL),
        (2, 'test', NULL, NULL, NULL, NULL),
        (3, NULL, 'test', NULL, NULL, NULL),
        (4, NULL, NULL, 'test', NULL, NULL),
        (5, NULL, NULL, NULL, 'test', NULL),
        (6, NULL, NULL, NULL, NULL, 'test'),

        -- Mixed NULL and non-NULL
        (7, 'apple', NULL, 'banana', NULL, 'cherry'),
        (8, NULL, 'apple', NULL, 'banana', NULL),
        (9, 'data', 'content', NULL, NULL, 'tag'),

        -- Special characters and empty strings
        (10, '', '', '', '', ''),
        (11, ' ', '  ', '   ', '    ', '     '),
        (12, 'special123', 'chars456', 'symbols789', 'pipe123', 'quotes456'),

        -- Repeated keywords across fields
        (13, 'keyword', 'keyword', 'keyword', 'keyword', 'keyword'),
        (14, 'target', 'different', 'target', 'other', 'target'),
        (15, 'unique1', 'unique2', 'unique3', 'unique4', 'unique5'),

        -- Large text content
        (16, 'large content field with many words including target keyword and other text',
             'another large field containing target multiple times target target',
             'medium field with target',
             'short target',
             'tiny'),

        -- Case variations
        (17, 'Target', 'TARGET', 'target', 'TaRgEt', 'target'),
        (18, 'Case', 'case', 'CASE', 'CaSe', 'casE')
    """

    // Wait for data to be ready
    Thread.sleep(5000)

    // Boundary Test 1: All NULL fields
    qt_boundary_1_all_null_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('field1:anything OR field2:anything OR field3:anything OR field4:anything OR field5:anything')
    """

    qt_boundary_1_all_null_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('field1:anything AND field2:anything AND field3:anything AND field4:anything AND field5:anything')
    """

    // Boundary Test 2: Single field NULL vs multiple fields NULL in OR
    qt_boundary_2_single_null_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName}
        WHERE search('field1:nonexistent OR field2:test')
        ORDER BY id
    """

    qt_boundary_2_multiple_null_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName}
        WHERE search('field1:nonexistent OR field2:test OR field3:nonexistent')
        ORDER BY id
    """

    // Boundary Test 3: NOT with various NULL combinations
    qt_boundary_3_not_null_field """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('NOT field1:test')
    """

    qt_boundary_3_external_not_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE not search('field1:test')
    """

    // Boundary Test 4: Empty string vs NULL handling
    qt_boundary_4_empty_string_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName}
        WHERE search('field1:""')
        ORDER BY id
    """

    qt_boundary_4_null_field_count """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE field1 IS NULL
    """

    qt_boundary_4_empty_field_count """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE field1 = ''
    """

    // Boundary Test 5: Complex nested boolean with NULLs
    qt_boundary_5_complex_nested """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('((field1:test OR field2:test) AND (field3:test OR field4:test)) OR field5:test')
    """

    qt_boundary_5_detailed_result """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, field1, field2, field3, field4, field5 FROM ${tableName}
        WHERE search('((field1:test OR field2:test) AND (field3:test OR field4:test)) OR field5:test')
        ORDER BY id
    """

    // Boundary Test 6: Large OR query with many NULL fields
    qt_boundary_6_large_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('field1:"target" OR field1:"keyword" OR field1:"apple" OR field1:"unique1" OR
                     field2:"target" OR field2:"keyword" OR field2:"apple" OR field2:"unique2" OR
                     field3:"target" OR field3:"keyword" OR field3:"banana" OR field3:"unique3" OR
                     field4:"target" OR field4:"keyword" OR field4:"banana" OR field4:"unique4" OR
                     field5:"target" OR field5:"keyword" OR field5:"cherry" OR field5:"unique5"')
    """

    // Boundary Test 7: Special characters and NULL interaction
    qt_boundary_7_special_chars_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('field1:special123 OR field2:nonexistent')
    """

    qt_boundary_7_special_chars_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('field1:special123 AND field2:chars456')
    """

    // Boundary Test 8: Case sensitivity with NULL fields
    qt_boundary_8_case_variations """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName}
        WHERE search('field1:Target OR field2:TARGET OR field3:target OR field4:TaRgEt')
        ORDER BY id
    """

    // Boundary Test 9: Multiple NOT operations
    qt_boundary_9_multiple_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('NOT (field1:nonexistent OR field2:nonexistent OR field3:nonexistent)')
    """

    qt_boundary_9_external_multiple_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE not search('field1:nonexistent OR field2:nonexistent OR field3:nonexistent')
    """

    // Boundary Test 10: Performance with NULL-heavy dataset
    qt_boundary_10_performance """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('(field1:test OR field1:target OR field1:keyword) AND
                     (field2:test OR field2:target OR field2:keyword) AND
                     NOT (field3:nonexistent OR field4:nonexistent OR field5:nonexistent)')
    """
}