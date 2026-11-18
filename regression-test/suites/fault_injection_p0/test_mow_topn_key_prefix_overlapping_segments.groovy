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
// a rowset by the ORDER BY prefix; rows that are equal on the prefix but differ in the rest of
// the key are different rows and must all come back. Before the fix the merge dropped them as
// older versions of one key: ORDER BY k1 LIMIT 10 returned k1 = 0..9 once each instead of
// k1 = 0..4 twice each.
suite("test_mow_topn_key_prefix_overlapping_segments", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_mow_topn_key_prefix_overlapping_segments"
    sql """
        CREATE TABLE test_mow_topn_key_prefix_overlapping_segments (k1 INT, k2 INT, v INT)
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true")
    """
    try {
        // MemTable.need_flush flushes every Block into its own segment: rows 0..19 and 20..39
        // become two segments of one rowset that both hold k1 = 0..19 (k1 = number % 20).
        GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
        sql "SET batch_size = 20"
        sql """
            INSERT INTO test_mow_topn_key_prefix_overlapping_segments
            SELECT number % 20, number, number FROM numbers("number" = "40")
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
    }
    sql "sync"
    sql "SET batch_size = 4064"

    // The test is void unless the load really wrote an OVERLAPPING rowset with two segments.
    def tablets = sql_return_maparray("SHOW TABLETS FROM test_mow_topn_key_prefix_overlapping_segments")
    def (code, out, err) = curl("GET", tablets[0].MetaUrl)
    assertEquals(0, code)
    def hasMergeLayout = parseJson(out.trim()).rs_metas.any { meta ->
        meta.segments_overlap_pb == "OVERLAPPING" && meta.num_segments > 1
    }
    assertTrue(hasMergeLayout, "expected an OVERLAPPING rowset with num_segments > 1")

    qt_count "SELECT count(*) FROM test_mow_topn_key_prefix_overlapping_segments"
    // Storage TopN by the key prefix k1: every k1 appears twice (k2 = k1 and k2 = k1 + 20).
    order_qt_topn_prefix "SELECT k1, k2 FROM test_mow_topn_key_prefix_overlapping_segments ORDER BY k1 LIMIT 10"
    // Storage TopN by the whole key.
    qt_topn_whole_key "SELECT k1, k2 FROM test_mow_topn_key_prefix_overlapping_segments ORDER BY k1, k2 LIMIT 10"
    // The same query without the storage TopN, for comparison.
    order_qt_no_storage_topn """
        SELECT /*+ SET_VAR(topn_opt_limit_threshold = 0) */ k1, k2
        FROM test_mow_topn_key_prefix_overlapping_segments ORDER BY k1 LIMIT 10
    """
}
