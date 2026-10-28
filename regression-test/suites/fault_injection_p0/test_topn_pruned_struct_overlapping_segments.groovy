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

// Regression case for the pruned-struct read misalignment on the inline topn scan path.
//
// Root cause: when a topn ordered read (order by key limit N) hits a rowset with
// multiple OVERLAPPING segments, BetaRowsetReader picks VMergeIterator, whose
// VMergeIteratorContext::init() eagerly loads the first block of every segment.
// That block is created from the storage Schema (FULL struct type, all children),
// while the struct sub-column iterators have been filtered down to the pruned
// subset by set_access_paths()/remove_pruned_sub_iterators(). The pairing loop in
// StructFileColumnIterator::next_batch() indexes _sub_column_iterators[i] by the
// dst tuple position, so the two ordinal spaces disagree:
//   - text/dict child data decoded into a TINYINT child column
//       -> [E-3110] insert_many_dict_data is not supported for TINYINT
//   - same-typed children shift silently, and the loop runs past the pruned
//     iterator vector -> out-of-bounds -> BE SIGSEGV
//
// The FE TopN lazy materialization (VMaterializeNode) usually shields this path
// on master; setting topn_lazy_materialization_threshold=-1 (or any state where
// the rule bails, e.g. light_schema_change=false tables) exposes it.
//
// Layout construction: MemTable.need_flush debug point forces one segment per
// inserted block (batch_size=500, 2000 rows -> 4 segments, below the
// segcompaction_batch_size=10 threshold so segcompaction never merges them), and
// the NULL k1 rows in every block make every segment's key lower bound NULL, so
// the segments overlap and the rowset is marked OVERLAPPING.
suite("test_topn_pruned_struct_overlapping_segments", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_topn_pruned_struct_ovlp_dict"
    sql "DROP TABLE IF EXISTS test_topn_pruned_struct_ovlp_int"
    sql """
        CREATE TABLE test_topn_pruned_struct_ovlp_dict (
            k1 BIGINT,
            c ARRAY<ARRAY<STRUCT<col1:INT, col2:TINYINT, col17:TEXT>>>
        ) DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES('replication_num' = '1', 'disable_auto_compaction' = 'true')
    """
    sql """
        CREATE TABLE test_topn_pruned_struct_ovlp_int (
            k1 BIGINT,
            c ARRAY<ARRAY<STRUCT<col1:INT, col2:INT, col3:INT>>>
        ) DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES('replication_num' = '1', 'disable_auto_compaction' = 'true')
    """

    try {
        GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
        // 4 blocks of 500 rows -> 4 segments per tablet; k1 NULL every 20 rows so
        // every segment starts at NULL -> overlapping key ranges -> OVERLAPPING rowset.
        sql "SET batch_size = 500"
        sql """
            INSERT INTO test_topn_pruned_struct_ovlp_dict
            SELECT if(number % 20 = 0, NULL, number),
                   array(array(named_struct(
                       'col1', CAST(number AS INT),
                       'col2', CAST(number % 127 AS TINYINT),
                       'col17', concat('s', number))))
            FROM numbers('number' = '2000')
        """
        sql """
            INSERT INTO test_topn_pruned_struct_ovlp_int
            SELECT if(number % 20 = 0, NULL, number),
                   array(array(named_struct(
                       'col1', CAST(number + 1000000 AS INT),
                       'col2', CAST(number + 2000000 AS INT),
                       'col3', CAST(number + 3000000 AS INT))))
            FROM numbers('number' = '2000')
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
    }

    // Self-check the storage layout: the test is void unless the rowset really is
    // a multi-segment OVERLAPPING one. BetaRowsetReader::is_merge_iterator() needs
    // both the OVERLAPPING flag and num_segments > 1, so assert both.
    for (def tbl : ["test_topn_pruned_struct_ovlp_dict", "test_topn_pruned_struct_ovlp_int"]) {
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

    // 1. Default session: on master this plans VMaterializeNode (TopN lazy
    //    materialization + rowid fetch); guards that path stays correct too.
    // Expected: k1 1..10 -> col1 = k1, col17 = 's' + k1.
    qt_dict_default_session """
        SELECT element_at(c[1][1], 1), element_at(c[1][1], 'col17')
        FROM test_topn_pruned_struct_ovlp_dict WHERE k1 IS NOT NULL ORDER BY k1 LIMIT 10
    """

    // 2. Force the inline topn scan path (no VMaterializeNode). This is the exact
    //    path that used to fail with
    //    "[E-3110] Method insert_many_dict_data is not supported for TINYINT".
    sql "SET topn_lazy_materialization_threshold = -1"
    // The storage ordered-key read (read_orderby_key -> need_ordered_result ->
    // VMergeIterator) is additionally gated by enable_segment_limit_pushdown in
    // OlapScanner. The variable is randomized in fuzzy regression runs; pin it so
    // this section always exercises the merge path this fix targets.
    sql "SET enable_segment_limit_pushdown = true"
    // Runtime filters disable storage TopN, and parallel scans can split the
    // overlapping segments into separate scanners that do not need a merge.
    sql "SET topn_filter_ratio = 0"
    sql "SET enable_parallel_scan = false"

    qt_dict_inline_topn """
        SELECT element_at(c[1][1], 1), element_at(c[1][1], 'col17')
        FROM test_topn_pruned_struct_ovlp_dict WHERE k1 IS NOT NULL ORDER BY k1 LIMIT 10
    """

    // Single pruned child: the misalignment shifts col17 data onto col1 (INT),
    // which used to fail with "... is not supported for INT".
    qt_dict_inline_topn_single_child """
        SELECT element_at(c[1][1], 'col17')
        FROM test_topn_pruned_struct_ovlp_dict WHERE k1 IS NOT NULL ORDER BY k1 LIMIT 10
    """

    // Same-typed children: the shifted decode does not error out, so the pairing
    // loop used to run past the pruned iterator vector and SIGSEGV the BE; a
    // partial regression could also return col1's values (1000001..) instead of
    // col3's (3000001..) here.
    qt_int_inline_topn_last_child """
        SELECT element_at(c[1][1], 'col3')
        FROM test_topn_pruned_struct_ovlp_int WHERE k1 IS NOT NULL ORDER BY k1 LIMIT 10
    """
}
