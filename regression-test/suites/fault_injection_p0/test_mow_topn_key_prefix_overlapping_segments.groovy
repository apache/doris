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

// ORDER BY <key prefix> LIMIT n pushed down to the storage layer on a merge-on-write table
// whose rowset has two segments that overlap on keys. The storage TopN merges the segments of
// a rowset by the ORDER BY columns; rows that are equal on them but differ in the rest of the
// key are different rows and must all come back. Before the fix the merge dropped them as
// older versions of one key: ORDER BY k1 LIMIT 10 returned k1 = 0..9 once each instead of
// k1 = 0..4 twice each.
suite("test_mow_topn_key_prefix_overlapping_segments", "nonConcurrent") {
    // Run `insertSql` with MemTable.need_flush on, which flushes every write into its own segment:
    // the 16384 rows of numbers("number" = "16384") become two segments of one rowset. One sink
    // task writes the Blocks in order, and a Block of 8192 rows is one write on both sinks: the
    // legacy sink sends a Block once it holds 8192 rows (VNodeChannel floors its batch size at
    // 8192), and the sink with the memtable on it writes every Block. The test is void unless the
    // load really wrote an OVERLAPPING rowset with two segments.
    def loadTwoOverlappingSegments = { String table, String insertSql ->
        try {
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
            sql "SET parallel_pipeline_task_num = 1"
            sql "SET batch_size = 8192"
            sql insertSql
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
        }
        sql "sync"
        def tablets = sql_return_maparray("SHOW TABLETS FROM ${table}")
        def (code, out, err) = curl("GET", tablets[0].MetaUrl)
        assertEquals(0, code)
        def hasMergeLayout = parseJson(out.trim()).rs_metas.any { meta ->
            meta.segments_overlap_pb == "OVERLAPPING" && meta.num_segments > 1
        }
        assertTrue(hasMergeLayout, "expected an OVERLAPPING rowset with num_segments > 1 in ${table}")
    }
    // The storage TopN below needs the segment limit pushdown, which the fuzzy session
    // variables may turn off.
    sql "SET enable_segment_limit_pushdown = true"

    // Both segments hold k1 = 0..8191: k2 = k1 in the first one, k2 = k1 + 8192 in the second.
    sql "DROP TABLE IF EXISTS test_mow_topn_key_prefix_overlapping_segments"
    sql """
        CREATE TABLE test_mow_topn_key_prefix_overlapping_segments (k1 INT, k2 INT, v INT)
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true")
    """
    loadTwoOverlappingSegments("test_mow_topn_key_prefix_overlapping_segments", """
        INSERT INTO test_mow_topn_key_prefix_overlapping_segments
        SELECT number % 8192, number, number FROM numbers("number" = "16384")
    """)
    // The TopN reaches the scan, so the storage layer reads it.
    explain {
        sql "SELECT k1, k2 FROM test_mow_topn_key_prefix_overlapping_segments ORDER BY k1 LIMIT 10"
        contains "SORT LIMIT: 10"
    }
    qt_count "SELECT count(*) FROM test_mow_topn_key_prefix_overlapping_segments"
    // Storage TopN by the key prefix k1: every k1 appears twice (k2 = k1 and k2 = k1 + 8192).
    order_qt_topn_prefix "SELECT k1, k2 FROM test_mow_topn_key_prefix_overlapping_segments ORDER BY k1 LIMIT 10"
    order_qt_topn_prefix_desc "SELECT k1, k2 FROM test_mow_topn_key_prefix_overlapping_segments ORDER BY k1 DESC LIMIT 10"
    // Storage TopN by the whole key.
    qt_topn_whole_key "SELECT k1, k2 FROM test_mow_topn_key_prefix_overlapping_segments ORDER BY k1, k2 LIMIT 10"
    // The same query without the storage TopN, for comparison.
    order_qt_no_storage_topn """
        SELECT /*+ SET_VAR(topn_opt_limit_threshold = 0) */ k1, k2
        FROM test_mow_topn_key_prefix_overlapping_segments ORDER BY k1 LIMIT 10
    """

    // Both segments hold the same full keys. The storage TopN keeps the rows equal on the ORDER BY
    // columns, and the delete bitmap of the merge-on-write table hides the older row of each key:
    // every key comes back once, with the value written last (v = k1 + 8192).
    sql "DROP TABLE IF EXISTS test_mow_topn_key_prefix_same_keys"
    sql """
        CREATE TABLE test_mow_topn_key_prefix_same_keys (k1 INT, k2 INT, v INT)
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true")
    """
    loadTwoOverlappingSegments("test_mow_topn_key_prefix_same_keys", """
        INSERT INTO test_mow_topn_key_prefix_same_keys
        SELECT number % 8192, number % 8192, number FROM numbers("number" = "16384")
    """)
    order_qt_same_keys_topn_prefix "SELECT k1, k2, v FROM test_mow_topn_key_prefix_same_keys ORDER BY k1 LIMIT 10"

    // A cluster key table pushes the TopN down by its cluster key, which identifies no row: both
    // segments hold v = 0..8191, with k = v in the first one and k = v + 8192 in the second.
    sql "DROP TABLE IF EXISTS test_mow_topn_cluster_key_prefix"
    sql """
        CREATE TABLE test_mow_topn_cluster_key_prefix (k INT, v INT, w INT)
        UNIQUE KEY(k)
        ORDER BY(v, w)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true")
    """
    loadTwoOverlappingSegments("test_mow_topn_cluster_key_prefix", """
        INSERT INTO test_mow_topn_cluster_key_prefix
        SELECT number, number % 8192, number FROM numbers("number" = "16384")
    """)
    explain {
        sql "SELECT k, v FROM test_mow_topn_cluster_key_prefix ORDER BY v LIMIT 10"
        contains "SORT LIMIT: 10"
    }
    order_qt_cluster_key_topn_prefix "SELECT k, v FROM test_mow_topn_cluster_key_prefix ORDER BY v LIMIT 10"
}
