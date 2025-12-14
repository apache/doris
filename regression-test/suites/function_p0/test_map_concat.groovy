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

suite("test_map_concat") {
    // Enable Nereids planner for testing
    sql """ set enable_nereids_planner=true; """
    sql """ set enable_fallback_to_original_planner=false; """

    def testTable = "test_map_concat_table"

    try {
        // Drop table if exists
        sql """
            drop table if exists ${testTable};
        """

        // Create test table with MAP columns
        sql """
        CREATE TABLE ${testTable} (
            id INT,
            map1 MAP<VARCHAR, VARCHAR>,
            map2 MAP<VARCHAR, INT>,
            map3 MAP<INT, VARCHAR>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

        // Insert test data
        sql """
            insert into ${testTable} values
            (1, {'a': 'apple', 'b': 'banana'}, {'x': 10, 'y': 20}, {1: 'one', 2: 'two'}),
            (2, {'c': 'cherry'}, {'z': 30}, {3: 'three'}),
            (3, {}, {}, {}),
            (4, NULL, NULL, NULL);
        """

        // ============================================
        // Group 1: Basic functionality tests
        // ============================================
        
        // Test 1.1: Basic map_concat with two maps (all rows)
        qt_sql """
            select 
                id,
                map_concat(map1, {'extra_key': 'extra_value'}) as merged_map
            from ${testTable}
            order by id;
        """

        // Test 1.2: Concatenating three columns from table
        qt_sql """
            select 
                id,
                map_concat(map1, map2, map3) as all_maps_merged
            from ${testTable}
            order by id;
        """

        // Test 1.3: Concatenating with literal maps (no table dependency)
        qt_sql """
            select 
                map_concat(
                    {'a': 'apple'}, 
                    {'b': 'banana'}, 
                    {'c': 'cherry'}
                ) as literal_maps_merged;
        """

        // ============================================
        // Group 2: Edge cases and special values
        // ============================================
        
        // Test 2.1: Concatenating with empty maps
        qt_sql """
            select 
                id,
                map_concat(map1, {}) as merged_with_empty1,
                map_concat({}, map2) as merged_with_empty2,
                map_concat({}, {}) as empty_with_empty
            from ${testTable}
            order by id;
        """

        // Test 2.2: Concatenating with NULL maps
        qt_sql """
            select 
                id,
                map_concat(map1, NULL) as map_with_null,
                map_concat(NULL, map2) as null_with_map,
                map_concat(NULL, NULL) as null_with_null
            from ${testTable}
            order by id;
        """

        // Test 2.3: Mixed NULL and empty maps
        qt_sql """
            select 
                map_concat(NULL, {}) as null_with_empty,
                map_concat({}, NULL) as empty_with_null;
        """

        // ============================================
        // Group 3: Key conflict and overwrite behavior
        // ============================================
        
        // Test 3.1: Key conflict (later value overwrites earlier)
        qt_sql """
            select 
                map_concat(
                    {'a': 'apple', 'b': 'banana'},
                    {'b': 'blueberry', 'c': 'cherry'}
                ) as conflict_resolution;
        """

        // Test 3.2: Multiple key conflicts across three maps
        qt_sql """
            select 
                map_concat(
                    {'a': 'first', 'b': 'first'},
                    {'b': 'second', 'c': 'second'},
                    {'c': 'third', 'd': 'third'}
                ) as multi_conflict;
        """

        // ============================================
        // Group 4: Function composition and expressions
        // ============================================
        
        // Test 4.1: Using map_concat in expressions with map_size
        qt_sql """
            select 
                id,
                map_size(map_concat(map1, map1)) as self_concat_size,
                map_size(map_concat(map1, map2)) as two_maps_size
            from ${testTable}
            order by id;
        """

        // Test 4.2: Nested map_concat operations
        qt_sql """
            select 
                map_concat(
                    map_concat({'a': 1}, {'b': 2}),
                    map_concat({'c': 3}, {'d': 4})
                ) as nested_concat;
        """

        // Test 4.3: map_concat with other map functions
        qt_sql """
            select 
                map_keys(map_concat({'x': 10}, {'y': 20})) as keys_result,
                map_values(map_concat({'x': 10}, {'y': 20})) as values_result;
        """

        // ============================================
        // Group 5: Type handling and error cases
        // ============================================
        
        // Test 5.1: Concatenating maps with same value types (should work)
        qt_sql """
            select 
                map_concat(
                    CAST({'a': '1'} AS MAP<VARCHAR, VARCHAR>),
                    CAST({'b': '2'} AS MAP<VARCHAR, VARCHAR>)
                ) as same_type_maps;
        """

        // Test 5.2: Testing with different key types (if supported)
        qt_sql """
            select 
                map_concat(
                    {1: 'one', 2: 'two'},
                    {3: 'three', 4: 'four'}
                ) as int_key_maps;
        """

        // Test 5.3: map_concat with many parameters (stress test)
        qt_sql """
            select 
                map_concat(
                    {'p1': 'v1'}, {'p2': 'v2'}, {'p3': 'v3'},
                    {'p4': 'v4'}, {'p5': 'v5'}, {'p6': 'v6'},
                    {'p7': 'v7'}, {'p8': 'v8'}, {'p9': 'v9'}
                ) as many_maps;
        """

        // Test 5.4: Error case - type mismatch (expected to fail)
        qt_sql """
            select 
                map_concat(
                    CAST({'a': 1} AS MAP<VARCHAR, INT>),
                    CAST({'b': '2'} AS MAP<VARCHAR, VARCHAR>)
                ) as type_mismatch;
        """

        // ============================================
        // Group 6: Additional edge cases and extended testing
        // ============================================
        
        // Test 6.1: Single argument (should return the same map)
        qt_sql """
            select 
                map_concat({'single': 'argument'}) as single_argument;
        """

        // Test 6.2: Map with NULL values inside
        qt_sql """
            select 
                map_concat(
                    {'a': 'value', 'b': NULL},
                    {'c': 'another', 'd': NULL}
                ) as maps_with_null_values;
        """

        // Test 6.3: Mixed NULL maps and empty maps
        qt_sql """
            select 
                map_concat(NULL, {'a': 1}, {}, NULL, {'b': 2}) as mixed_nulls_empties;
        """

        // Test 6.4: More numeric types testing
        qt_sql """
            select 
                map_concat(
                    CAST({'tiny': 127} AS MAP<VARCHAR, TINYINT>),
                    CAST({'small': 32767} AS MAP<VARCHAR, SMALLINT>),
                    CAST({'int': 2147483647} AS MAP<VARCHAR, INT>)
                ) as more_numeric_types;
        """

        // Test 6.5: Boolean type values
        qt_sql """
            select 
                map_concat(
                    {'flag1': true, 'flag2': false},
                    {'flag3': true}
                ) as boolean_values;
        """

        // Test 6.6: Date type values
        qt_sql """
            select 
                map_concat(
                    {'date1': DATE '2023-01-01'},
                    {'date2': DATE '2023-12-31'}
                ) as date_values;
        """

        // Test 6.7: Array as map values
        qt_sql """
            select 
                map_concat(
                    {'arr1': [1, 2, 3]},
                    {'arr2': [4, 5, 6]}
                ) as array_values;
        """

        // Test 6.8: Very many parameters (more than 9)
        qt_sql """
            select 
                map_concat(
                    {'k01': 'v01'}, {'k02': 'v02'}, {'k03': 'v03'},
                    {'k04': 'v04'}, {'k05': 'v05'}, {'k06': 'v06'},
                    {'k07': 'v07'}, {'k08': 'v08'}, {'k09': 'v09'},
                    {'k10': 'v10'}, {'k11': 'v11'}, {'k12': 'v12'}
                ) as many_parameters;
        """

        // Test 6.9: Nested map_concat operations (deep nesting)
        qt_sql """
            select 
                map_concat(
                    map_concat({'a': 1}, {'b': 2}),
                    map_concat(
                        map_concat({'c': 3}, {'d': 4}),
                        map_concat({'e': 5}, {'f': 6})
                    )
                ) as deeply_nested;
        """

        // Test 6.10: map_concat with map_contains_key combination
        qt_sql """
            select 
                map_contains_key(
                    map_concat({'a': 1, 'b': 2}, {'c': 3, 'd': 4}),
                    'c'
                ) as contains_key_c,
                map_contains_key(
                    map_concat({'a': 1, 'b': 2}, {'c': 3, 'd': 4}),
                    'e'
                ) as contains_key_e;
        """

        // Test 6.11: map_concat in WHERE clause
        qt_sql """
            select 
                id,
                map_concat(map1, map2) as merged
            from ${testTable}
            where map_size(map_concat(map1, map2)) > 0
            order by id;
        """

        // Test 6.12: Key order consistency test
        qt_sql """
            select 
                map_keys(map_concat({'z': 1, 'a': 2}, {'m': 3})) as keys_order1,
                map_keys(map_concat({'a': 2, 'z': 1}, {'m': 3})) as keys_order2;
        """

        // ============================================
        // Group 7: Advanced edge cases - interface, charset, and nested scenarios
        // ============================================
        
        // Test 7.1: Interface-related test - map_concat with map_entries
        qt_sql """
            select 
                map_entries(map_concat({'a': 1, 'b': 2}, {'c': 3, 'd': 4})) as entries_result;
        """

        // Test 7.2: Interface-related test - map_concat with map_contains_value
        qt_sql """
            select 
                map_contains_value(map_concat({'a': 1, 'b': 2}, {'c': 3, 'd': 4}), 3) as contains_value_3,
                map_contains_value(map_concat({'a': 1, 'b': 2}, {'c': 3, 'd': 4}), 5) as contains_value_5;
        """

        // Test 7.3: Different charset test - UTF-8 special characters
        qt_sql """
            select 
                map_concat(
                    {'中文键': '中文值', 'key with emoji 🔥': 'value with emoji 🚀'},
                    {'key with accents café': 'value with accents naïve'}
                ) as utf8_charset_test;
        """

        // Test 7.4: Different charset test - mixed language keys
        qt_sql """
            select 
                map_concat(
                    {'English key': 'value1', '日本語キー': '値1'},
                    {'한국어 키': '값1', 'русский ключ': 'значение1'}
                ) as mixed_language_test;
        """

        // Test 7.5: Nested map_concat - complex nesting
        qt_sql """
            select 
                map_concat(
                    map_concat(
                        map_concat({'a': 1}, {'b': 2}),
                        map_concat({'c': 3}, {'d': 4})
                    ),
                    map_concat(
                        map_concat(
                            map_concat({'e': 5}, {'f': 6}),
                            map_concat({'g': 7}, {'h': 8})
                        ),
                        map_concat({'i': 9}, {'j': 10})
                    )
                ) as complex_nested_concat;
        """

        // Test 7.6: Nested map_concat with different key types
        qt_sql """
            select 
                map_concat(
                    map_concat({1: 'one', 2: 'two'}, {3: 'three', 4: 'four'}),
                    map_concat({'five': 5, 'six': 6}, {'seven': 7, 'eight': 8})
                ) as mixed_key_types_nested;
        """

        // Test 7.7: Interface test - map_concat in subquery
        qt_sql """
            select 
                id,
                (select map_concat({'static': 'value'}, map1) from ${testTable} where id = t.id) as subquery_concat
            from ${testTable} t
            order by id;
        """

        // Test 7.8: Interface test - map_concat with CASE expression
        qt_sql """
            select 
                id,
                map_concat(
                    CASE WHEN id % 2 = 0 THEN {'even': 'true'} ELSE {'odd': 'true'} END,
                    CASE WHEN id = 1 THEN {'id': '1'} 
                         WHEN id = 2 THEN {'id': '2'} 
                         WHEN id = 3 THEN {'id': '3'} 
                         ELSE {'id': '4'} END
                ) as case_expr_concat
            from ${testTable}
            order by id;
        """

        // Test 7.9: Special characters in keys and values
        qt_sql """
            select 
                map_concat(
                    {'key with "double quotes"': 'value with ''single quotes'''},
                    {'key with \\\\backslashes\\\\': 'value with \\\\n newline'},
                    {'key with , comma': 'value with ; semicolon'},
                    {'key with {} braces': 'value with [] brackets'}
                ) as special_chars_test;
        """

        // Test 7.10: Empty string as key and value
        qt_sql """
            select 
                map_concat(
                    {'': 'empty key', 'empty value': ''},
                    {'': 'another empty key', 'key': ''}
                ) as empty_string_test;
        """

        // Test 7.11: Very long strings as keys and values
        qt_sql """
            select 
                map_concat(
                    {'very_long_key_very_long_key_very_long_key_very_long_key_very_long_key_very_long_key_very_long_key_very_long_key_very_long_key_very_long_key_': 
                     'very_long_value_very_long_value_very_long_value_very_long_value_very_long_value_very_long_value_very_long_value_very_long_value_very_long_value_very_long_value_'},
                    {'another_long_key_another_long_key_another_long_key_another_long_key_another_long_key_another_long_key_another_long_key_another_long_key_another_long_key_another_long_key_another_long_key_another_long_key_another_long_key_another_long_key_': 
                     'another_long_value_another_long_value_another_long_value_another_long_value_another_long_value_another_long_value_another_long_value_another_long_value_another_long_value_another_long_value_another_long_value_another_long_value_another_long_value_another_long_value_'}
                ) as long_strings_test;
        """

        // Test 7.12: map_concat with COALESCE to handle NULLs
        qt_sql """
            select 
                map_concat(
                    COALESCE(map1, {}),
                    COALESCE(map2, {})
                ) as coalesce_handled
            from ${testTable}
            order by id;
        """
    } finally {
        // Clean up
        try {
            sql """
                drop table if exists ${testTable};
            """
        } catch (Exception e) {
            println "Error cleaning up table ${testTable}: ${e.message}"
        }
    }
}
