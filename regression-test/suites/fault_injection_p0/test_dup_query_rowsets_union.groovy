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

// With the BE config enable_new_block_iterator on, a query of a DUP_KEYS table that does not ask
// for key order reads its rowsets one after another, each through its rowset reader. The table
// below gets a rowset whose two segments overlap on keys (one load that flushed two memtables), two
// more rowsets and a DELETE. It is read with Blocks of 3 rows, with a filter, with a LIMIT pushed
// down to every segment and with aggregates, and again after a full compaction rewrites the
// rowsets into one.
suite("test_dup_query_rowsets_union", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_dup_query_rowsets_union"
    sql """
        CREATE TABLE test_dup_query_rowsets_union (
            k INT,
            v BIGINT
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """

    try {
        // MemTable.need_flush flushes every Block into its own segment. With Blocks of 20 rows,
        // numbers 0..19 and 20..39 become two segments that both hold keys 0..19
        // (k = number % 20).
        GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
        sql "SET batch_size = 20"
        sql """
            INSERT INTO test_dup_query_rowsets_union
            SELECT number % 20, number FROM numbers("number" = "40")
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
    }
    sql "INSERT INTO test_dup_query_rowsets_union VALUES (5, 100), (25, 101)"
    sql "INSERT INTO test_dup_query_rowsets_union VALUES (30, 102)"
    sql "DELETE FROM test_dup_query_rowsets_union WHERE k = 7"
    sql "sync"

    def tablets = sql_return_maparray("SHOW TABLETS FROM test_dup_query_rowsets_union")
    def (code, out, err) = curl("GET", tablets[0].MetaUrl)
    assertEquals(0, code)
    def hasOverlappingRowset = parseJson(out.trim()).rs_metas.any { meta ->
        meta.segments_overlap_pb == "OVERLAPPING" && meta.num_segments > 1
    }
    assertTrue(hasOverlappingRowset,
            "expected an OVERLAPPING rowset with num_segments > 1, "
                    + "check the MemTable.need_flush debug point")

    // One scanner reads every rowset of the tablet; the LIMIT goes down to the segments.
    sql "SET enable_parallel_scan = false"
    sql "SET enable_segment_limit_pushdown = true"
    explain {
        sql "SELECT k, v FROM test_dup_query_rowsets_union"
        contains "PREAGGREGATION: ON"
    }

    // Fuzzy BE configs may turn the new block iterators off, and with them the union of the
    // rowsets.
    setBeConfigTemporary([enable_new_block_iterator: true]) {
        sql "SET batch_size = 3"
        order_qt_all "SELECT k, v FROM test_dup_query_rowsets_union"
        order_qt_filter "SELECT k, v FROM test_dup_query_rowsets_union WHERE v >= 30"
        qt_limit "SELECT count(*) FROM (SELECT k FROM test_dup_query_rowsets_union LIMIT 7) t"
        qt_agg "SELECT count(*), min(k), max(k), sum(v) FROM test_dup_query_rowsets_union"

        trigger_and_wait_compaction("test_dup_query_rowsets_union", "full")
        def (compacted_code, compacted_out, compacted_err) = curl("GET", tablets[0].MetaUrl)
        assertEquals(0, compacted_code)
        assertEquals(1, parseJson(compacted_out.trim()).rs_metas.size(),
                "expected the full compaction to leave one rowset")
        order_qt_all_after_compaction "SELECT k, v FROM test_dup_query_rowsets_union"
    }
}
