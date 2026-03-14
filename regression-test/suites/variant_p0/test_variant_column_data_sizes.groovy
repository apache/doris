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

suite("test_variant_column_data_sizes", "p0") {
    if(isCloudMode()) {
        return
    }
    
    def getTableId = {tableName ->
        def result = sql """
            SELECT table_id FROM information_schema.metadata_name_ids WHERE table_name = '${tableName}'
        """
        assertTrue(result.size() > 0, "Table ${tableName} should exist in metadata_name_ids")
        return result[0][0]
    }
    
    def getVariantDataSizes = {tableId ->
        sql """
            SELECT SUM(RAW_DATA_BYTES) as raw,
                   SUM(COMPRESSED_DATA_BYTES) as compressed,
                   SUM(UNCOMPRESSED_DATA_BYTES) as uncompressed
            FROM information_schema.column_data_sizes
            WHERE TABLE_ID = ${tableId} AND COLUMN_NAME = 'v'
        """
    }
    
    // ============================================================
    // Test Group 1: VariantColumnWriterImpl + UnifiedSparseColumnWriter (Normal Mode)
    // ============================================================
    
    def table1 = "test_variant_normal_sparse"
    sql "DROP TABLE IF EXISTS ${table1}"
    sql """
        CREATE TABLE ${table1} (
            id INT NOT NULL,
            v VARIANT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "false",
            "compression" = "ZSTD"
        )
    """
    
    sql """
        INSERT INTO ${table1} VALUES
        (1, '{"a": 1, "b": "text1", "c": 1.5}'),
        (2, '{"a": 2, "b": "text2", "c": 2.5, "d": true}'),
        (3, '{"b": "text3", "d": false, "e": null}'),
        (4, '{"a": 4, "b": "text4"}')
    """
    
    sql """
        INSERT INTO ${table1}
        SELECT number, CAST(concat('{"path_', number % 10, '": "value_', number, '"}') AS VARIANT)
        FROM numbers("number" = "65536")
    """
    sql "SYNC"
    
    def tableId1 = getTableId(table1)
    def sizes1 = getVariantDataSizes(tableId1)
    assertTrue(sizes1.size() > 0, "Should have data sizes for variant column")
    assertTrue(sizes1[0][0] > 0, "Group 1: raw_data_bytes should be > 0")
    assertTrue(sizes1[0][1] > 0, "Group 1: compressed_data_bytes should be > 0")
    assertTrue(sizes1[0][2] > 0, "Group 1: uncompressed_data_bytes should be > 0")
    assertTrue(sizes1[0][1] <= sizes1[0][2], "Group 1: compressed should be <= uncompressed")
    
    sql "DROP TABLE IF EXISTS ${table1}"
    
    // ============================================================
    // Test Group 2: VariantDocWriter (Doc Value Mode, No Compaction)
    // ============================================================
    
    sql """ set default_variant_enable_doc_mode = true """
    
    def table2 = "test_variant_docwriter_no_compact"
    sql "DROP TABLE IF EXISTS ${table2}"
    sql """
        CREATE TABLE ${table2} (
            id INT NOT NULL,
            v VARIANT<properties("variant_enable_doc_mode"="true")>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "compression" = "ZSTD"
        )
    """
    
    sql """
        INSERT INTO ${table2} VALUES
        (1, '{"x": 100, "y": 200}'),
        (2, '{"x": 101, "y": 201, "z": "nested"}'),
        (3, '{"y": 202, "z": "val"}')
    """
    
    sql """
        INSERT INTO ${table2}
        SELECT number, CAST(concat('{"id":', number, ', "data": "', repeat('d', 50), '"}') AS VARIANT<properties("variant_enable_doc_mode"="true")>)
        FROM numbers("number" = "65536")
    """
    sql "SYNC"
    
    def tableId2 = getTableId(table2)
    def sizes2 = getVariantDataSizes(tableId2)
    assertTrue(sizes2.size() > 0, "Should have data sizes for variant column")
    assertTrue(sizes2[0][0] > 0, "Group 2: raw_data_bytes should be > 0 (no compaction)")
    assertTrue(sizes2[0][1] > 0, "Group 2: compressed_data_bytes should be > 0")
    assertTrue(sizes2[0][2] > 0, "Group 2: uncompressed_data_bytes should be > 0")
    assertTrue(sizes2[0][1] <= sizes2[0][2], "Group 2: compressed should be <= uncompressed")
    
    sql "DROP TABLE IF EXISTS ${table2}"
    
    // ============================================================
    // Test Group 3: VariantDocCompactWriter (Compaction with Doc Value)
    // ============================================================
    
    def table3 = "test_variant_doccompact"
    sql "DROP TABLE IF EXISTS ${table3}"
    sql """
        CREATE TABLE ${table3} (
            id INT NOT NULL,
            v VARIANT<properties("variant_enable_doc_mode"="true","variant_doc_materialization_min_rows"="0")>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "false",
            "compression" = "ZSTD"
        )
    """
    
    sql """
        INSERT INTO ${table3} VALUES
        (1, '{"field1": 1, "field2": "a"}'),
        (2, '{"field1": 2, "field2": "b"}')
    """
    
    sql """
        INSERT INTO ${table3}
        SELECT number, 
               CAST(concat('{"field1":', number, ', "field2": "', repeat('x', 30), '", "field3":', number * 2, '}') AS VARIANT<properties("variant_enable_doc_mode"="true","variant_doc_materialization_min_rows"="0")>)
        FROM numbers("number" = "65536")
    """
    sql "SYNC"
    trigger_and_wait_compaction(table3, "full")
    
    def tableId3 = getTableId(table3)
    def sizes3 = getVariantDataSizes(tableId3)
    assertTrue(sizes3.size() > 0, "Should have data sizes for variant column")
    assertTrue(sizes3[0][1] > 0, "Group 3: compressed_data_bytes should be > 0")
    assertTrue(sizes3[0][2] > 0, "Group 3: uncompressed_data_bytes should be > 0")
    assertTrue(sizes3[0][1] <= sizes3[0][2], "Group 3: compressed should be <= uncompressed")
    sql "DROP TABLE IF EXISTS ${table3}"
    
    // ============================================================
    // Test Group 4: Sparse Mode with Multiple Buckets (UnifiedSparseColumnWriter)
    // ============================================================
    
    sql """ set default_variant_enable_doc_mode = false """
    
    def table4 = "test_variant_sparse_buckets"
    sql "DROP TABLE IF EXISTS ${table4}"
    sql """
        CREATE TABLE ${table4} (
            id INT NOT NULL,
            v VARIANT<properties("variant_sparse_hash_shard_count"="4")>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "false",
            "compression" = "ZSTD"
        )
    """
    
    sql """
        INSERT INTO ${table4}
        SELECT number, CAST(concat('{"path_', number % 20, '": "value_', number, '"}') AS VARIANT<properties("variant_sparse_hash_shard_count"="4")>)
        FROM numbers("number" = "65536")
    """
    sql "SYNC"
    
    def tableId4 = getTableId(table4)
    def sizes4 = getVariantDataSizes(tableId4)
    assertTrue(sizes4.size() > 0, "Should have data sizes for variant column")
    assertTrue(sizes4[0][0] > 0, "Group 4: raw_data_bytes should be > 0")
    assertTrue(sizes4[0][1] > 0, "Group 4: compressed_data_bytes should be > 0")
    assertTrue(sizes4[0][2] > 0, "Group 4: uncompressed_data_bytes should be > 0")
    assertTrue(sizes4[0][1] <= sizes4[0][2], "Group 4: compressed should be <= uncompressed")
    sql "DROP TABLE IF EXISTS ${table4}"
    
    // ============================================================
    // Test Group 5: Complex Nested Structures
    // ============================================================
    
    def table5 = "test_variant_complex"
    sql "DROP TABLE IF EXISTS ${table5}"
    sql """
        CREATE TABLE ${table5} (
            id INT NOT NULL,
            v VARIANT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "false",
            "compression" = "ZSTD"
        )
    """
    
    sql """
        INSERT INTO ${table5} VALUES
        (1, '{"nested": {"deep": {"value": 1}}}'),
        (2, '{"array": [1, 2, 3]}'),
        (3, '{"mixed": {"field1": 10, "field2": "text", "field3": [1,2,3]}}')
    """
    
    sql """
        INSERT INTO ${table5}
        SELECT number, 
               CAST(concat('{"level1": {"level2": {"level3": "value_', number, '"}}}') AS VARIANT)
        FROM numbers("number" = "65536")
    """
    sql "SYNC"
    
    def tableId5 = getTableId(table5)
    def sizes5 = getVariantDataSizes(tableId5)
    assertTrue(sizes5.size() > 0, "Should have data sizes for variant column")
    assertTrue(sizes5[0][0] > 0, "Group 5: raw_data_bytes should be > 0")
    assertTrue(sizes5[0][1] > 0, "Group 5: compressed_data_bytes should be > 0")
    assertTrue(sizes5[0][2] > 0, "Group 5: uncompressed_data_bytes should be > 0")
    assertTrue(sizes5[0][1] <= sizes5[0][2], "Group 5: compressed should be <= uncompressed")

    sql "DROP TABLE IF EXISTS ${table5}"
    
    // ============================================================
    // Final Verification
    // ============================================================
    
    assertTrue(true, "All variant writer interface tests passed")
    
    sql """ set default_variant_enable_doc_mode = false """
}
