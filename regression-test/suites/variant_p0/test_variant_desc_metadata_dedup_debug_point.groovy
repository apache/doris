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

suite("test_variant_desc_metadata_dedup_debug_point", "p0, nonConcurrent") {
    String debugPoint = "VariantCompactionUtil.calculate_variant_extended_schema.check_scan_segments"

    sql "DROP TABLE IF EXISTS test_variant_desc_metadata_dedup_debug_point"
    sql "SET default_variant_enable_doc_mode = false"
    sql "SET default_variant_max_subcolumns_count = 10"
    sql "SET default_variant_enable_typed_paths_to_sparse = false"
    sql "SET default_variant_sparse_hash_shard_count = 0"
    sql """
        CREATE TABLE test_variant_desc_metadata_dedup_debug_point (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "10")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """INSERT INTO test_variant_desc_metadata_dedup_debug_point VALUES (1, '{"a": 1, "b": "x"}')"""
    sql """INSERT INTO test_variant_desc_metadata_dedup_debug_point VALUES (2, '{"a": 2, "b": "y"}')"""
    sql """INSERT INTO test_variant_desc_metadata_dedup_debug_point VALUES (3, '{"a": 3, "b": "z"}')"""
    sql "sync"
    sql "SELECT * FROM test_variant_desc_metadata_dedup_debug_point LIMIT 1"
    sql "SET describe_extend_variant_column = true"

    def tablets = sql_return_maparray "SHOW TABLETS FROM test_variant_desc_metadata_dedup_debug_point"
    assertEquals(1, tablets.size())
    def rowsets = sql_return_maparray """
        SELECT ROWSET_ID, NUM_SEGMENTS
        FROM information_schema.rowsets
        WHERE TABLET_ID = ${tablets[0].TabletId} AND ROWSET_NUM_ROWS > 0
        ORDER BY START_VERSION
    """
    assertEquals(3, rowsets.size())
    rowsets.each { rowset ->
        assertEquals(1, rowset.NUM_SEGMENTS as int)
    }

    def rows = sql "DESC test_variant_desc_metadata_dedup_debug_point"
    def fieldNames = rows.collect { it[0].toString() }
    assertTrue(fieldNames.contains("v.a"), "DESC fields: ${fieldNames}")
    assertTrue(fieldNames.contains("v.b"), "DESC fields: ${fieldNames}")

    try {
        GetDebugPoint().enableDebugPointForAllBEs(debugPoint)
        rows = sql "DESC test_variant_desc_metadata_dedup_debug_point"
        fieldNames = rows.collect { it[0].toString() }
        assertTrue(fieldNames.contains("v.a"), "DESC fields: ${fieldNames}")
        assertTrue(fieldNames.contains("v.b"), "DESC fields: ${fieldNames}")
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(debugPoint)
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs(
                debugPoint, [selected_rowsets: "0", selected_segments: "0"])
        rows = sql "DESC test_variant_desc_metadata_dedup_debug_point"
        fieldNames = rows.collect { it[0].toString() }
        assertEquals(["k", "v"], fieldNames)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(debugPoint)
    }
}
