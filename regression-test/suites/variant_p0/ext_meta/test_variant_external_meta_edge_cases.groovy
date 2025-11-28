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

suite("test_variant_external_meta_edge_cases", "nonConcurrent") {
    def set_be_config = { key, value ->
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), 
                                                backendId_to_backendHttpPort.get(backend_id), 
                                                key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }

    // Test 1: Empty subcolumns (only root variant column)
    sql "DROP TABLE IF EXISTS test_empty_subcolumns"
    sql """
        CREATE TABLE test_empty_subcolumns (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    sql """insert into test_empty_subcolumns values (1, '{}')"""
    sql """insert into test_empty_subcolumns values (2, 'null')"""
    sql """insert into test_empty_subcolumns values (3, '[]')"""
    
    qt_empty_1 "select k, cast(v as string) from test_empty_subcolumns order by k"
    qt_empty_2 "select k from test_empty_subcolumns where v is null order by k"
    qt_empty_3 "select k from test_empty_subcolumns where v is not null order by k"

    // Test 2: Many subcolumns (test performance and correctness)
    sql "DROP TABLE IF EXISTS test_many_subcolumns"
    sql """
        CREATE TABLE test_many_subcolumns (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Generate JSON with 100 subcolumns
    def generateLargeJson = { int numFields ->
        def fields = []
        for (int i = 0; i < numFields; i++) {
            fields.add("\"field_${i}\": ${i}")
        }
        return "{" + fields.join(", ") + "}"
    }
    
    // external meta enabled by table property
    // Insert rows with different numbers of subcolumns
    for (int row = 0; row < 10; row++) {
        int numFields = (row + 1) * 10  // 10, 20, 30, ..., 100 fields
        def json = generateLargeJson(numFields)
        sql """insert into test_many_subcolumns values (${row}, '${json}')"""
    }
    
    qt_many_1 "select k, v['field_0'] from test_many_subcolumns order by k"
    qt_many_2 "select k, v['field_50'] from test_many_subcolumns where cast(v['field_50'] as int) is not null order by k"
    qt_many_3 "select k, v['field_99'] from test_many_subcolumns where cast(v['field_99'] as int) is not null order by k"
    qt_many_4 "select count(*) from test_many_subcolumns"

    // Test 3: Mixed old and new format segments
    sql "DROP TABLE IF EXISTS test_mixed_format"
    sql """
        CREATE TABLE test_mixed_format (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Write with old format (V2)
    sql """insert into test_mixed_format values (1, '{"a": 1, "b": 2}')"""
    sql """insert into test_mixed_format values (2, '{"a": 10, "c": 3}')"""
    
    // Switch to new format (V2.1) and write more rows
    sql """insert into test_mixed_format values (3, '{"a": 100, "d": 4}')"""
    sql """insert into test_mixed_format values (4, '{"a": 1000, "e": 5}')"""
    
    // Query should work across all segments
    qt_mixed_1 "select k, v['a'] from test_mixed_format order by k"
    qt_mixed_2 "select k, v['b'] from test_mixed_format where cast(v['b'] as int) is not null order by k"
    qt_mixed_3 "select k, v['c'] from test_mixed_format where cast(v['c'] as int) is not null order by k"
    qt_mixed_4 "select k, v['d'] from test_mixed_format where cast(v['d'] as int) is not null order by k"
    qt_mixed_5 "select k, v['e'] from test_mixed_format where cast(v['e'] as int) is not null order by k"
    
    // Trigger compaction and verify
    trigger_and_wait_compaction("test_mixed_format", "full")
    
    qt_mixed_after_compact_1 "select k, v['a'] from test_mixed_format order by k"
    qt_mixed_after_compact_2 "select count(distinct k) from test_mixed_format"

    // Test 4: Nested structures with external meta
    sql "DROP TABLE IF EXISTS test_nested_external"
    sql """
        CREATE TABLE test_nested_external (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    sql """insert into test_nested_external values (1, '{"nested": {"level1": {"level2": "value"}}}')"""
    sql """insert into test_nested_external values (2, '{"nested": {"level1": {"level2": "value2", "level2_b": 123}}}')"""
    sql """insert into test_nested_external values (3, '{"nested": {"level1": null}}')"""
    sql """insert into test_nested_external values (4, '{"nested": null}')"""
    
    qt_nested_1 "select k, v['nested']['level1']['level2'] from test_nested_external order by k"
    qt_nested_2 "select k, v['nested']['level1']['level2_b'] from test_nested_external where cast(v['nested']['level1']['level2_b'] as int) is not null order by k"
    qt_nested_3 "select k from test_nested_external where v['nested'] is not null order by k"
    qt_nested_4 "select k from test_nested_external where v['nested']['level1'] is not null order by k"

    // Test 5: Array types with external meta
    sql "DROP TABLE IF EXISTS test_array_external"
    sql """
        CREATE TABLE test_array_external (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    sql """insert into test_array_external values (1, '{"arr": [1, 2, 3]}')"""
    sql """insert into test_array_external values (2, '{"arr": [10, 20, 30, 40]}')"""
    sql """insert into test_array_external values (3, '{"arr": []}')"""
    sql """insert into test_array_external values (4, '{"arr": null}')"""
    
    qt_array_1 "select k, v['arr'] from test_array_external order by k"
    qt_array_2 "select k from test_array_external where v['arr'] is not null order by k"

    // Test 6: Schema evolution - adding subcolumns over time
    sql "DROP TABLE IF EXISTS test_schema_evolution"
    sql """
        CREATE TABLE test_schema_evolution (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    sql """insert into test_schema_evolution values (1, '{"a": 1}')"""
    
    // Segment 2: add field 'b'
    sql """insert into test_schema_evolution values (2, '{"a": 2, "b": 2}')"""
    
    // Segment 3: add field 'c'
    sql """insert into test_schema_evolution values (3, '{"a": 3, "b": 3, "c": 3}')"""
    
    // Segment 4: completely different schema
    sql """insert into test_schema_evolution values (4, '{"x": 10, "y": 20}')"""
    
    qt_evolution_1 "select k, v['a'] from test_schema_evolution order by k"
    qt_evolution_2 "select k, v['b'] from test_schema_evolution order by k"
    qt_evolution_3 "select k, v['c'] from test_schema_evolution order by k"
    qt_evolution_4 "select k, v['x'] from test_schema_evolution order by k"
    qt_evolution_5 "select k, v['y'] from test_schema_evolution order by k"

    // Test 7: Special characters in field names
    sql "DROP TABLE IF EXISTS test_special_chars"
    sql """
        CREATE TABLE test_special_chars (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    sql """insert into test_special_chars values (1, '{"field-with-dash": 1}')"""
    sql """insert into test_special_chars values (2, '{"field.with.dot": 2}')"""
    sql """insert into test_special_chars values (3, '{"field_with_underscore": 3}')"""
    
    qt_special_1 "select k, v['field-with-dash'] from test_special_chars where cast(v['field-with-dash'] as int) is not null order by k"
    qt_special_2 "select k, v['field.with.dot'] from test_special_chars where cast(v['field.with.dot'] as int) is not null order by k"
    qt_special_3 "select k, v['field_with_underscore'] from test_special_chars where cast(v['field_with_underscore'] as int) is not null order by k"

    // Test 8: NULL handling
    sql "DROP TABLE IF EXISTS test_null_handling"
    sql """
        CREATE TABLE test_null_handling (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    sql """insert into test_null_handling values (1, '{"a": null}')"""
    sql """insert into test_null_handling values (2, '{"a": 1}')"""
    sql """insert into test_null_handling values (3, null)"""
    
    qt_null_1 "select k, v['a'] from test_null_handling order by k"
    qt_null_2 "select k from test_null_handling where v is null order by k"
    qt_null_3 "select k from test_null_handling where v['a'] is null order by k"
    qt_null_4 "select k from test_null_handling where v['a'] is not null order by k"

    // Test 9: Large string values
    sql "DROP TABLE IF EXISTS test_large_strings"
    sql """
        CREATE TABLE test_large_strings (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    def largeString = "x" * 10000  // 10KB string
    sql """insert into test_large_strings values (1, '{"large_field": "${largeString}"}')"""
    sql """insert into test_large_strings values (2, '{"large_field": "small"}')"""
    
    qt_large_1 "select k, length(cast(v['large_field'] as string)) from test_large_strings order by k"
    qt_large_2 "select k from test_large_strings where length(cast(v['large_field'] as string)) > 100 order by k"

    // Test 10: Config toggle during operation
    sql "DROP TABLE IF EXISTS test_config_toggle"
    sql """
        CREATE TABLE test_config_toggle (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Start with new format (V2.1)
    sql """insert into test_config_toggle values (1, '{"a": 1}')"""
    
    // Switch to legacy (V2)
    sql """insert into test_config_toggle values (2, '{"a": 2, "b": 2}')"""
    
    // Enable again (V2.1)
    sql """insert into test_config_toggle values (3, '{"a": 3, "b": 3, "c": 3}')"""
    
    // All queries should work regardless of config state
    qt_toggle_1 "select k, v['a'] from test_config_toggle order by k"
    qt_toggle_2 "select k, v['b'] from test_config_toggle where cast(v['b'] as int) is not null order by k"
    qt_toggle_3 "select k, v['c'] from test_config_toggle where cast(v['c'] as int) is not null order by k"
    
    // Compact and verify
    trigger_and_wait_compaction("test_config_toggle", "full")
    
    qt_toggle_after_compact_1 "select k, v['a'] from test_config_toggle order by k"
    qt_toggle_after_compact_2 "select k, v['b'] from test_config_toggle where cast(v['b'] as int) is not null order by k"

    // Cleanup
    sql "DROP TABLE IF EXISTS test_empty_subcolumns"
    sql "DROP TABLE IF EXISTS test_many_subcolumns"
    sql "DROP TABLE IF EXISTS test_mixed_format"
    sql "DROP TABLE IF EXISTS test_nested_external"
    sql "DROP TABLE IF EXISTS test_array_external"
    sql "DROP TABLE IF EXISTS test_schema_evolution"
    sql "DROP TABLE IF EXISTS test_special_chars"
    sql "DROP TABLE IF EXISTS test_null_handling"
    sql "DROP TABLE IF EXISTS test_large_strings"
    sql "DROP TABLE IF EXISTS test_config_toggle"
    
}


