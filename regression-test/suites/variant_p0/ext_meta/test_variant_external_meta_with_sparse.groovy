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

suite("test_variant_external_meta_with_sparse", "nonConcurrent") {
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

    // Test 1: Sparse columns should be embedded in root's children_columns
    sql "DROP TABLE IF EXISTS test_sparse_embedded"
    sql """
        CREATE TABLE test_sparse_embedded (
            k bigint,
            v variant<properties("variant_max_subcolumns_count" = "3")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Insert data that will exceed subcolumn limit, creating sparse columns
    sql """insert into test_sparse_embedded values (1, '{"a": 1, "b": 2, "c": 3}')"""
    sql """insert into test_sparse_embedded values (2, '{"a": 10, "b": 20, "c": 30, "d": 40}')"""
    sql """insert into test_sparse_embedded values (3, '{"a": 100, "e": 500, "f": 600, "g": 700}')"""
    
    // Query extracted columns (should be in external meta)
    qt_sparse_1 "select k, v['a'] from test_sparse_embedded order by k"
    qt_sparse_2 "select k, v['b'] from test_sparse_embedded where cast(v['b'] as int) is not null order by k"
    qt_sparse_3 "select k, v['c'] from test_sparse_embedded where cast(v['c'] as int) is not null order by k"
    
    // Query sparse columns (should be embedded in root's children_columns)
    qt_sparse_4 "select k, v['d'] from test_sparse_embedded where cast(v['d'] as int) is not null order by k"
    qt_sparse_5 "select k, v['e'] from test_sparse_embedded where cast(v['e'] as int) is not null order by k"
    qt_sparse_6 "select k, v['f'] from test_sparse_embedded where cast(v['f'] as int) is not null order by k"
    qt_sparse_7 "select k, v['g'] from test_sparse_embedded where cast(v['g'] as int) is not null order by k"
    
    // Compaction should handle both extracted and sparse columns correctly
    trigger_and_wait_compaction("test_sparse_embedded", "full", 1800)
    
    qt_sparse_after_compact_1 "select k, v['a'] from test_sparse_embedded order by k"
    qt_sparse_after_compact_2 "select k, v['d'] from test_sparse_embedded where cast(v['d'] as int) is not null order by k"
    qt_sparse_after_compact_3 "select k, v['g'] from test_sparse_embedded where cast(v['g'] as int) is not null order by k"

    // Test 2: Bucketed sparse columns with external meta
    sql "DROP TABLE IF EXISTS test_bucket_sparse"
    sql """
        CREATE TABLE test_bucket_sparse (
            k bigint,
            v variant<properties("variant_max_subcolumns_count" = "2")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Create multiple rows that will generate bucketed sparse columns
    for (int i = 0; i < 10; i++) {
        def fields = ["\"extracted_1\": ${i}", "\"extracted_2\": ${i * 10}"]
        // Add varying sparse fields
        for (int j = 0; j < 5; j++) {
            fields.add("\"sparse_${i}_${j}\": ${i * 100 + j}")
        }
        def json = "{" + fields.join(", ") + "}"
        sql """insert into test_bucket_sparse values (${i}, '${json}')"""
    }
    
    // Query extracted columns
    qt_bucket_1 "select k, v['extracted_1'] from test_bucket_sparse order by k"
    qt_bucket_2 "select k, v['extracted_2'] from test_bucket_sparse order by k"
    
    // Query sparse columns from different rows
    qt_bucket_3 "select k, v['sparse_0_0'] from test_bucket_sparse where cast(v['sparse_0_0'] as int) is not null order by k"
    qt_bucket_4 "select k, v['sparse_5_3'] from test_bucket_sparse where cast(v['sparse_5_3'] as int) is not null order by k"
    qt_bucket_5 "select count(*) from test_bucket_sparse"
    
    // Compact and verify buckets are handled correctly
    trigger_and_wait_compaction("test_bucket_sparse", "full", 1800)
    
    qt_bucket_after_compact_1 "select k, v['extracted_1'] from test_bucket_sparse order by k"
    qt_bucket_after_compact_2 "select k, v['sparse_0_0'] from test_bucket_sparse where cast(v['sparse_0_0'] as int) is not null order by k"

    // Test 3: Mixed extracted, sparse, and non-existent columns
    sql "DROP TABLE IF EXISTS test_mixed_column_types"
    sql """
        CREATE TABLE test_mixed_column_types (
            k bigint,
            v variant<properties("variant_max_subcolumns_count" = "5")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Row 1: Only extracted columns
    sql """insert into test_mixed_column_types values (1, '{"e1": 1, "e2": 2, "e3": 3, "e4": 4, "e5": 5}')"""
    
    // Row 2: Extracted + sparse
    sql """insert into test_mixed_column_types values (2, '{"e1": 10, "e2": 20, "e3": 30, "e4": 40, "e5": 50, "s1": 60, "s2": 70}')"""
    
    // Row 3: Different set of fields
    sql """insert into test_mixed_column_types values (3, '{"e1": 100, "new1": 200, "new2": 300}')"""
    
    // Query all types
    qt_mixed_1 "select k, v['e1'] from test_mixed_column_types order by k"  // Exists in all rows (extracted)
    qt_mixed_2 "select k, v['e5'] from test_mixed_column_types where cast(v['e5'] as int) is not null order by k"  // Exists in row 1,2 (extracted)
    qt_mixed_3 "select k, v['s1'] from test_mixed_column_types where cast(v['s1'] as int) is not null order by k"  // Exists in row 2 (sparse)
    qt_mixed_4 "select k, v['new1'] from test_mixed_column_types where cast(v['new1'] as int) is not null order by k"  // Exists in row 3 (sparse or extracted)
    qt_mixed_5 "select k, v['non_existent'] from test_mixed_column_types order by k"  // Doesn't exist anywhere

    // Test 4: Transition from extracted to sparse
    sql "DROP TABLE IF EXISTS test_extraction_threshold"
    sql """
        CREATE TABLE test_extraction_threshold (
            k bigint,
            v variant<properties("variant_max_subcolumns_count" = "3")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Segment 1: Within extraction limit
    sql """insert into test_extraction_threshold values (1, '{"field_a": 1, "field_b": 2}')"""
    sql """insert into test_extraction_threshold values (2, '{"field_a": 10, "field_b": 20, "field_c": 30}')"""
    
    // Segment 2: Exceeds limit, triggers sparse columns
    sql """insert into test_extraction_threshold values (3, '{"field_a": 100, "field_b": 200, "field_c": 300, "field_d": 400}')"""
    sql """insert into test_extraction_threshold values (4, '{"field_a": 1000, "field_e": 5000}')"""
    
    // All fields should be queryable
    qt_threshold_1 "select k, v['field_a'] from test_extraction_threshold order by k"
    qt_threshold_2 "select k, v['field_b'] from test_extraction_threshold where cast(v['field_b'] as int) is not null order by k"
    qt_threshold_3 "select k, v['field_c'] from test_extraction_threshold where cast(v['field_c'] as int) is not null order by k"
    qt_threshold_4 "select k, v['field_d'] from test_extraction_threshold where cast(v['field_d'] as int) is not null order by k"
    qt_threshold_5 "select k, v['field_e'] from test_extraction_threshold where cast(v['field_e'] as int) is not null order by k"
    
    // After compaction, schema should be merged correctly
    trigger_and_wait_compaction("test_extraction_threshold", "full", 1800)
    
    qt_threshold_after_compact_1 "select k, v['field_a'] from test_extraction_threshold order by k"
    qt_threshold_after_compact_2 "select k, v['field_d'] from test_extraction_threshold where cast(v['field_d'] as int) is not null order by k"
    qt_threshold_after_compact_3 "select k, v['field_e'] from test_extraction_threshold where cast(v['field_e'] as int) is not null order by k"

    // Test 5: Sparse columns with config toggle
    sql "DROP TABLE IF EXISTS test_sparse_config_toggle"
    sql """
        CREATE TABLE test_sparse_config_toggle (
            k bigint,
            v variant<properties("variant_max_subcolumns_count" = "2")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Write with external meta enabled (V2.1)
    sql """insert into test_sparse_config_toggle values (1, '{"a": 1, "b": 2, "c": 3, "d": 4}')"""
    
    // Switch to legacy V2
    sql """insert into test_sparse_config_toggle values (2, '{"a": 10, "b": 20, "c": 30, "e": 50}')"""
    
    // Enable external meta again (V2.1)
    sql """insert into test_sparse_config_toggle values (3, '{"a": 100, "f": 600, "g": 700}')"""
    
    // All queries should work regardless of which format was used
    qt_toggle_sparse_1 "select k, v['a'] from test_sparse_config_toggle order by k"
    qt_toggle_sparse_2 "select k, v['b'] from test_sparse_config_toggle where cast(v['b'] as int) is not null order by k"
    qt_toggle_sparse_3 "select k, v['c'] from test_sparse_config_toggle where cast(v['c'] as int) is not null order by k"
    qt_toggle_sparse_4 "select k, v['d'] from test_sparse_config_toggle where cast(v['d'] as int) is not null order by k"
    qt_toggle_sparse_5 "select k, v['e'] from test_sparse_config_toggle where cast(v['e'] as int) is not null order by k"
    qt_toggle_sparse_6 "select k, v['f'] from test_sparse_config_toggle where cast(v['f'] as int) is not null order by k"
    
    // Compact
    trigger_and_wait_compaction("test_sparse_config_toggle", "full", 1800)
    
    qt_toggle_sparse_after_compact_1 "select k, v['a'] from test_sparse_config_toggle order by k"
    qt_toggle_sparse_after_compact_2 "select k, v['d'] from test_sparse_config_toggle where cast(v['d'] as int) is not null order by k"
    qt_toggle_sparse_after_compact_3 "select count(*) from test_sparse_config_toggle"

    // Test 6: Very high cardinality sparse columns
    sql "DROP TABLE IF EXISTS test_high_cardinality_sparse"
    sql """
        CREATE TABLE test_high_cardinality_sparse (
            k bigint,
            v variant<properties("variant_max_subcolumns_count" = "1")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Each row has one extracted column and many unique sparse columns
    for (int i = 0; i < 20; i++) {
        def fields = ["\"common\": ${i}"]
        // Each row has unique sparse fields
        for (int j = 0; j < 10; j++) {
            fields.add("\"unique_${i}_${j}\": ${i * 100 + j}")
        }
        def json = "{" + fields.join(", ") + "}"
        sql """insert into test_high_cardinality_sparse values (${i}, '${json}')"""
    }
    
    // Query common extracted field
    qt_high_cardinality_1 "select k, v['common'] from test_high_cardinality_sparse order by k limit 5"
    
    // Query sparse fields (each exists in only one row)
    qt_high_cardinality_2 "select k, v['unique_0_0'] from test_high_cardinality_sparse where cast(v['unique_0_0'] as int) is not null order by k"
    qt_high_cardinality_3 "select k, v['unique_10_5'] from test_high_cardinality_sparse where cast(v['unique_10_5'] as int) is not null order by k"
    qt_high_cardinality_4 "select count(*) from test_high_cardinality_sparse"
    
    // Compaction with high cardinality sparse columns
    trigger_and_wait_compaction("test_high_cardinality_sparse", "full", 1800)
    
    qt_high_cardinality_after_compact_1 "select k, v['common'] from test_high_cardinality_sparse order by k limit 5"
    qt_high_cardinality_after_compact_2 "select count(*) from test_high_cardinality_sparse"

    // Cleanup
    sql "DROP TABLE IF EXISTS test_sparse_embedded"
    sql "DROP TABLE IF EXISTS test_bucket_sparse"
    sql "DROP TABLE IF EXISTS test_mixed_column_types"
    sql "DROP TABLE IF EXISTS test_extraction_threshold"
    sql "DROP TABLE IF EXISTS test_sparse_config_toggle"
    sql "DROP TABLE IF EXISTS test_high_cardinality_sparse"
    
    
}


