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

// One load that flushes several memtables writes a rowset whose segments overlap on keys, and
// reading it merges the segments together with the rowsets. The AGG_KEYS table below gets one
// such rowset in which every key sits in both segments, and the second segment holds the newer
// rows: every key folds into one row, and REPLACE takes the newer segment's value. The queries
// read with Blocks of 3 rows, so key groups are cut by Block boundaries, and the results must
// stay the same after a full compaction rewrites the rowset.
suite("test_overlapping_segments_merge", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_overlapping_segments_merge_agg"
    sql """
        CREATE TABLE test_overlapping_segments_merge_agg (
            k INT,
            s BIGINT SUM,
            r INT REPLACE
        ) AGGREGATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """

    try {
        // MemTable.need_flush flushes every Block into its own segment. With Blocks of 20 rows,
        // numbers 0..19 and 20..39 become two segments that both hold keys 0..19
        // (k = number % 20), and the second segment holds the newer rows.
        GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
        sql "SET batch_size = 20"
        sql """
            INSERT INTO test_overlapping_segments_merge_agg
            SELECT number % 20, 1, number FROM numbers("number" = "40")
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
    }
    sql "sync"

    // The test is void unless the load really wrote an OVERLAPPING rowset with several segments:
    // only then are segments merged.
    def tablets = sql_return_maparray("SHOW TABLETS FROM test_overlapping_segments_merge_agg")
    def (code, out, err) = curl("GET", tablets[0].MetaUrl)
    assertEquals(0, code)
    def hasMergeLayout = parseJson(out.trim()).rs_metas.any { meta ->
        meta.segments_overlap_pb == "OVERLAPPING" && meta.num_segments > 1
    }
    assertTrue(hasMergeLayout,
            "expected an OVERLAPPING rowset with num_segments > 1, "
                    + "check the MemTable.need_flush debug point")

    // Without an aggregation above it, the scan of the AGG_KEYS table folds the rows itself.
    explain {
        sql "SELECT k, s, r FROM test_overlapping_segments_merge_agg"
        contains "PREAGGREGATION: OFF"
    }

    // The segment merge yields 0 0 1 1 ... 19 19; Blocks of 3 rows cut every other key group.
    sql "SET batch_size = 3"
    order_qt_agg "SELECT k, s, r FROM test_overlapping_segments_merge_agg"

    // A second rowset gives the full compaction something to merge; it rewrites both rowsets
    // into one.
    sql "INSERT INTO test_overlapping_segments_merge_agg VALUES (20, 1, 20)"
    trigger_and_wait_compaction("test_overlapping_segments_merge_agg", "full")
    def (compacted_code, compacted_out, compacted_err) = curl("GET", tablets[0].MetaUrl)
    assertEquals(0, compacted_code)
    assertEquals(1, parseJson(compacted_out.trim()).rs_metas.size(),
            "expected the full compaction to leave one rowset")
    order_qt_agg_after_compaction "SELECT k, s, r FROM test_overlapping_segments_merge_agg"
}
