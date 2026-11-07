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
// reading it merges the segments before the rowsets. Each table below gets one such rowset in
// which every key sits in both segments, and the second segment holds the newer rows:
//   - AGG_KEYS: every key folds into one row, REPLACE takes the newer segment's value;
//   - merge-on-read UNIQUE_KEYS: the newer segment's row wins;
//   - UNIQUE_KEYS with a sequence map: each column group takes the row with its own largest
//     sequence value, which is in a different segment for each group.
// The queries read with Blocks of 3 rows, so key groups are cut by Block boundaries, and the
// results must stay the same after a full compaction rewrites the rowset.
suite("test_overlapping_segments_merge", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_overlapping_segments_merge_agg"
    sql "DROP TABLE IF EXISTS test_overlapping_segments_merge_uniq"
    sql "DROP TABLE IF EXISTS test_overlapping_segments_merge_seq_map"
    sql """
        CREATE TABLE test_overlapping_segments_merge_agg (
            k INT,
            s BIGINT SUM,
            r INT REPLACE
        ) AGGREGATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql """
        CREATE TABLE test_overlapping_segments_merge_uniq (
            k INT,
            v INT
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        CREATE TABLE test_overlapping_segments_merge_seq_map (
            k INT,
            v1 INT,
            s1 BIGINT,
            v2 INT,
            s2 BIGINT
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change" = "true",
            "sequence_mapping.s1" = "v1",
            "sequence_mapping.s2" = "v2",
            "disable_auto_compaction" = "true"
        )
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
        sql """
            INSERT INTO test_overlapping_segments_merge_uniq
            SELECT number % 20, number FROM numbers("number" = "40")
        """
        // s1 is larger in the first segment and s2 in the second one, so v1 must come from the
        // older row and v2 from the newer row of every key.
        sql """
            INSERT INTO test_overlapping_segments_merge_seq_map
            SELECT number % 20, number, if(number < 20, 2, 1), number, if(number < 20, 1, 2)
            FROM numbers("number" = "40")
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
    }
    sql "sync"

    def tables = ["test_overlapping_segments_merge_agg", "test_overlapping_segments_merge_uniq",
                  "test_overlapping_segments_merge_seq_map"]
    // The test is void unless the load really wrote an OVERLAPPING rowset with several segments:
    // only then does the rowset reader merge segments.
    for (def tbl : tables) {
        def tablets = sql_return_maparray("SHOW TABLETS FROM ${tbl}")
        def (code, out, err) = curl("GET", tablets[0].MetaUrl)
        assertEquals(0, code)
        def jsonMeta = parseJson(out.trim())
        def hasMergeLayout = jsonMeta.rs_metas.any { meta ->
            meta.segments_overlap_pb == "OVERLAPPING" && meta.num_segments > 1
        }
        assertTrue(hasMergeLayout,
                "expected an OVERLAPPING rowset with num_segments > 1 in ${tbl}, "
                        + "check the MemTable.need_flush debug point")
    }

    // Without an aggregation above it, the scan of the AGG_KEYS table folds the rows itself.
    explain {
        sql "SELECT k, s, r FROM test_overlapping_segments_merge_agg"
        contains "PREAGGREGATION: OFF"
    }

    // The segment merge yields 0 0 1 1 ... 19 19; Blocks of 3 rows cut every other key group.
    sql "SET batch_size = 3"
    order_qt_agg "SELECT k, s, r FROM test_overlapping_segments_merge_agg"
    order_qt_uniq "SELECT k, v FROM test_overlapping_segments_merge_uniq"
    order_qt_seq_map "SELECT k, v1, s1, v2, s2 FROM test_overlapping_segments_merge_seq_map"

    // A second rowset gives the full compaction something to merge; it rewrites both rowsets
    // into one, reading the overlapping segments the same way.
    sql "INSERT INTO test_overlapping_segments_merge_agg VALUES (20, 1, 20)"
    sql "INSERT INTO test_overlapping_segments_merge_uniq VALUES (20, 20)"
    sql "INSERT INTO test_overlapping_segments_merge_seq_map VALUES (20, 20, 1, 20, 1)"
    for (def tbl : tables) {
        trigger_and_wait_compaction(tbl, "full")
        def tablets = sql_return_maparray("SHOW TABLETS FROM ${tbl}")
        def (code, out, err) = curl("GET", tablets[0].MetaUrl)
        assertEquals(0, code)
        def rsMetas = parseJson(out.trim()).rs_metas
        assertEquals(1, rsMetas.size(), "expected the full compaction to leave one rowset in ${tbl}")
    }
    order_qt_agg_after_compaction "SELECT k, s, r FROM test_overlapping_segments_merge_agg"
    order_qt_uniq_after_compaction "SELECT k, v FROM test_overlapping_segments_merge_uniq"
    order_qt_seq_map_after_compaction """
        SELECT k, v1, s1, v2, s2 FROM test_overlapping_segments_merge_seq_map
    """
}
