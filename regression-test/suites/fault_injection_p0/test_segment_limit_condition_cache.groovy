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

// A storage TopN (ORDER BY key LIMIT n) stops each segment after n rows. It must not leave
// a condition cache entry behind, or a later query with the same predicate skips row ranges
// that do have matching rows.
//
// The bug needs a rowset with overlapping segments, which is read by VMergeIterator: it asks
// every segment for one more batch, so a segment that hit its LIMIT reaches EOF.
// MemTable.need_flush writes one segment per inserted block.
suite("test_segment_limit_condition_cache", "nonConcurrent") {
    def tbl = "test_segment_limit_condition_cache"
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql """
        CREATE TABLE ${tbl} (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num' = '1', 'disable_auto_compaction' = 'true')
    """

    try {
        GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
        // 4 blocks of 4096 rows -> 4 segments, each larger than one 2048-row cache granule.
        // Every block has keys from the whole range, so the segments overlap.
        sql "SET batch_size = 4096"
        sql """
            INSERT INTO ${tbl}
            SELECT (number * 7919) % 16384, (number * 7919) % 16384 % 7 - 3
            FROM numbers('number' = '16384')
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
    }

    def tablets = sql_return_maparray("SHOW TABLETS FROM ${tbl}")
    def (code, out, err) = curl("GET", tablets[0].MetaUrl)
    assertEquals(0, code)
    def hasMergeLayout = parseJson(out.trim()).rs_metas.any { meta ->
        meta.segments_overlap_pb == "OVERLAPPING" && meta.num_segments > 1
    }
    assertTrue(hasMergeLayout, "expected an OVERLAPPING rowset with more than one segment")

    // Both switches are randomized in fuzzy runs; this case needs both on.
    sql "SET enable_condition_cache = true"
    sql "SET enable_segment_limit_pushdown = true"
    // Disable TopN runtime filters so the queries exercise storage TopN and condition cache.
    sql "SET topn_filter_ratio = 0"
    explain {
        sql "SELECT k, v FROM ${tbl} WHERE abs(v) > 0 ORDER BY k LIMIT 1"
        contains "SORT LIMIT: 1"
        notContains "TOPN filter targets"
    }

    // Each case runs the TopN first, then the same predicate without LIMIT.
    qt_topn_asc "SELECT k, v FROM ${tbl} WHERE abs(v) > 0 ORDER BY k LIMIT 1"
    qt_all_after_asc "SELECT count(*), sum(k) FROM ${tbl} WHERE abs(v) > 0"

    qt_topn_desc "SELECT k, v FROM ${tbl} WHERE abs(v) > 1 ORDER BY k DESC LIMIT 1"
    qt_all_after_desc "SELECT count(*), sum(k) FROM ${tbl} WHERE abs(v) > 1"
}
