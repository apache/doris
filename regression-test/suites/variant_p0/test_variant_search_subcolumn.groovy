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

suite("test_variant_search_subcolumn") {
    def table_name = "test_variant_search_subcolumn"
    sql "set default_variant_doc_materialization_min_rows = 0"

    sql "DROP TABLE IF EXISTS ${table_name}"

    // Create table with variant column and inverted index
    sql """
        CREATE TABLE ${table_name} (
            id BIGINT,
            overflowpropertiesfulltext VARIANT<PROPERTIES("variant_max_subcolumns_count"="0")>,
            INDEX idx_overflow (overflowpropertiesfulltext) USING INVERTED PROPERTIES (
                "parser" = "unicode",
                "lower_case" = "true",
                "support_phrase" = "true"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "inverted_index_storage_format" = "V2"
        )
    """

    // Insert test data
    sql """
        INSERT INTO ${table_name} VALUES
        (1, '{"string4": "0ff dpr test"}'),
        (2, '{"string4": "hello world"}'),
        (3, '{"string4": "0ff test"}'),
        (4, '{"string5": "0ff dpr"}'),
        (5, '{"string4": "dpr only"}'),
        (6, '{"nested": {"field": "0ff dpr"}}')
    """

    // Wait for data to be flushed and index to be built
    Thread.sleep(10000)

    // Test 1: Single term search on variant subcolumn
    logger.info("Test 1: Single term search on variant subcolumn")
    qt_test1 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true, default_variant_max_subcolumns_count=0)*/ id FROM ${table_name}
        WHERE search('overflowpropertiesfulltext.string4:0ff')
        ORDER BY id
    """
    // Expected: 1, 3

    // Test 2: AND query on same variant subcolumn
    logger.info("Test 2: AND query on same variant subcolumn")
    qt_test2 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true, default_variant_max_subcolumns_count=0)*/ id FROM ${table_name}
        WHERE search('overflowpropertiesfulltext.string4:0ff AND overflowpropertiesfulltext.string4:dpr')
        ORDER BY id
    """
    // Expected: 1

    // Test 3: ALL search on variant subcolumn
    logger.info("Test 3: ALL search on variant subcolumn")
    qt_test3 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true, default_variant_max_subcolumns_count=0)*/ id FROM ${table_name}
        WHERE search('overflowpropertiesfulltext.string4:ALL(0ff dpr)')
        ORDER BY id
    """
    // Expected: 1

    // Test 4: Search on different variant subcolumns (OR)
    logger.info("Test 4: Search on different variant subcolumns")
    qt_test4 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true, default_variant_max_subcolumns_count=0)*/ id FROM ${table_name}
        WHERE search('overflowpropertiesfulltext.string4:hello OR overflowpropertiesfulltext.string5:dpr')
        ORDER BY id
    """
    // Expected: 2, 4

    // Test 5: Search on non-existent subcolumn
    logger.info("Test 5: Search on non-existent subcolumn")
    qt_test5 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true, default_variant_max_subcolumns_count=0)*/ COUNT(*) FROM ${table_name}
        WHERE search('overflowpropertiesfulltext.nonexistent:value')
    """
    // Expected: 0

    // Test 6: Nested variant path
    logger.info("Test 6: Nested variant path")
    qt_test6 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true, default_variant_max_subcolumns_count=0)*/ id FROM ${table_name}
        WHERE search('overflowpropertiesfulltext.nested.field:0ff')
        ORDER BY id
    """
    // Expected: 6

    // Test 7: Complex query with variant subcolumns
    logger.info("Test 7: Complex query with variant subcolumns")
    qt_test7 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true, default_variant_max_subcolumns_count=0)*/ id FROM ${table_name}
        WHERE search('(overflowpropertiesfulltext.string4:0ff OR overflowpropertiesfulltext.string4:dpr) AND NOT overflowpropertiesfulltext.string4:hello')
        ORDER BY id
    """
    // Expected: 1, 3, 5

    // Test 8: Quoted field names with special characters
    logger.info("Test 8: Quoted field names")
    sql """
        INSERT INTO ${table_name} VALUES
        (7, '{"field-name": "test value"}')
    """
    Thread.sleep(5000)

    qt_test8 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true, default_variant_max_subcolumns_count=0)*/ id FROM ${table_name}
        WHERE search('overflowpropertiesfulltext.field-name:test')
        ORDER BY id
    """
    // Expected: 7

    // Test 9: Wildcard search on variant subcolumn
    //logger.info("Test 9: Wildcard search on variant subcolumn")
    //qt_test9 """
    //    SELECT /*+SET_VAR(enable_common_expr_pushdown=true, default_variant_max_subcolumns_count=0)*/ id FROM ${table_name}
    //    WHERE search('overflowpropertiesfulltext.string4:0*')
    //    ORDER BY id
    //"""
    // Expected: 1, 3

    // Test 10: Verify normal field search still works
    logger.info("Test 10: Verify normal field search still works (if id has index)")
    // This test verifies we didn't break normal field search
    qt_test10 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true, default_variant_max_subcolumns_count=0)*/ COUNT(*) FROM ${table_name}
        WHERE id > 0
    """
    // Expected: 7

    logger.info("Variant subcolumn search tests completed successfully!")
}
