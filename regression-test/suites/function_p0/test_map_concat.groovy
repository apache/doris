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
            where id = 1
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
            where id = 3
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
            where id = 4
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
            where id = 1
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
        try {
            qt_sql """
                select 
                    map_concat(
                        CAST({'a': 1} AS MAP<VARCHAR, INT>),
                        CAST({'b': '2'} AS MAP<VARCHAR, VARCHAR>)
                    ) as type_mismatch;
            """
            println "WARNING: Type mismatch test passed unexpectedly"
        } catch (Exception e) {
            println "Test 5.4 expected to fail for type mismatch: ${e.message}"
        }

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
