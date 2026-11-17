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
// which every key sits in both segments, and the later segment holds the newer rows:
//   - AGG_KEYS: every key folds into one row, REPLACE takes the newer segment's value;
//   - merge-on-read UNIQUE_KEYS: the newer segment's row wins;
//   - UNIQUE_KEYS with a sequence map: each column group takes the row with its own largest
//     sequence value, which is in a different segment for each group.
// The expected AGG_KEYS and UNIQUE_KEYS rows come from the rows in storage order, so they do not
// depend on which Block the load writes first. The queries read with Blocks of 3 rows, so key groups
// are cut by Block boundaries, and the results must stay the same after a full compaction rewrites
// the rowset.
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
        // MemTable.need_flush flushes every write into its own segment. One sink task writes
        // numbers 0..8191 and 8192..16383 as two Blocks of 8192 rows, and a Block of 8192 rows is
        // one write on both sinks: the legacy sink, the only one in cloud mode, sends a Block once
        // it holds 8192 rows (VNodeChannel floors its batch size at 8192), and the sink with the
        // memtable on it writes every Block. The memtable folds each write into keys 0..19
        // (k = number % 20), so the two segments both hold every key. A value is k in the rows of
        // numbers 0..8191 and k + 20 in those of 8192..16383, the same in every row of a key that
        // one write folds.
        GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
        sql "SET parallel_pipeline_task_num = 1"
        sql "SET batch_size = 8192"
        sql """
            INSERT INTO test_overlapping_segments_merge_agg
            SELECT number % 20, 1, if(number < 8192, number % 20, number % 20 + 20)
            FROM numbers("number" = "16384")
        """
        sql """
            INSERT INTO test_overlapping_segments_merge_uniq
            SELECT number % 20, if(number < 8192, number % 20, number % 20 + 20)
            FROM numbers("number" = "16384")
        """
        // s1 is larger in the rows of numbers 0..8191 and s2 in those of 8192..16383, so v1 and v2
        // of every key come from different rows, whichever segment holds them.
        sql """
            INSERT INTO test_overlapping_segments_merge_seq_map
            SELECT number % 20, if(number < 8192, number % 20, number % 20 + 20),
                   if(number < 8192, 2, 1), if(number < 8192, number % 20, number % 20 + 20),
                   if(number < 8192, 1, 2)
            FROM numbers("number" = "16384")
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

    // The newer row of a key is the one in the later segment: read the rows in storage order,
    // without the merge, to know it.
    sql "SET skip_storage_engine_merge = true"
    sql "SET parallel_pipeline_task_num = 1"
    def aggRows = sql "SELECT k, s, r FROM test_overlapping_segments_merge_agg"
    def uniqRows = sql "SELECT k, v FROM test_overlapping_segments_merge_uniq"
    sql "SET skip_storage_engine_merge = false"
    assertEquals(40, aggRows.size(), "expected both rows of every key without the merge")
    assertEquals(40, uniqRows.size(), "expected both rows of every key without the merge")
    def expectedAgg = aggRows.groupBy { it[0] }
            .collect { k, rows -> [k, rows.sum { it[1] }, rows.last()[2]] }
            .sort { it[0] }
    def expectedUniq = uniqRows.groupBy { it[0] }.collect { k, rows -> rows.last() }.sort { it[0] }

    // The segment merge yields 0 0 1 1 ... 19 19; Blocks of 3 rows cut every other key group.
    sql "SET batch_size = 3"
    assertEquals(expectedAgg, sql("SELECT k, s, r FROM test_overlapping_segments_merge_agg ORDER BY k"))
    assertEquals(expectedUniq, sql("SELECT k, v FROM test_overlapping_segments_merge_uniq ORDER BY k"))
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
    assertEquals(expectedAgg + [[20, 1L, 20]],
            sql("SELECT k, s, r FROM test_overlapping_segments_merge_agg ORDER BY k"))
    assertEquals(expectedUniq + [[20, 20]],
            sql("SELECT k, v FROM test_overlapping_segments_merge_uniq ORDER BY k"))
    order_qt_seq_map_after_compaction """
        SELECT k, v1, s1, v2, s2 FROM test_overlapping_segments_merge_seq_map
    """
}
