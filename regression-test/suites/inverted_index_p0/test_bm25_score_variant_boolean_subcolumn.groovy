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

// DORIS-25510: BM25 score() collection used to abort the BE when a variant
// sub-column had been written as a numeric/BKD index in some segments
// (e.g. {"c": false}) while the cloned parent index_meta still described
// a fulltext index. IndexReader::open() threw
// "No segments* file found in DorisCompoundReader@..." and the exception
// escaped collection_statistics.cpp:process_segment(), triggering SIGABRT.
suite("test_bm25_score_variant_boolean_subcolumn", "p0") {
    if (isCloudMode()) {
        return
    }

    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_match_without_inverted_index = false """

    sql "DROP TABLE IF EXISTS test_bm25_score_variant_boolean_subcolumn"
    sql """
        CREATE TABLE test_bm25_score_variant_boolean_subcolumn (
            `id` int(11) NULL,
            `v` variant NULL,
            INDEX idx_v (`v`) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "disable_auto_compaction" = "true"
        )
    """

    // Three single-row inserts -> three separate segments. Only segment 3
    // materializes the `v.c` sub-column, and there it is a boolean (so the
    // segment-level index is BKD, not fulltext).
    sql """ insert into test_bm25_score_variant_boolean_subcolumn values(1, '{"a": "abc"}') """
    sql """ insert into test_bm25_score_variant_boolean_subcolumn values(2, '{"b": "abc"}') """
    sql """ insert into test_bm25_score_variant_boolean_subcolumn values(3, '{"c": false}') """
    sql " sync "

    // Before the fix this aborts the BE; afterwards it must succeed and
    // return zero rows because no segment has v.c indexed as a string term
    // matching "abc".
    qt_score_match_boolean_subcolumn """
        select id, score()
        from test_bm25_score_variant_boolean_subcolumn
        where v["c"] match "abc"
        order by score(), id
        limit 10
    """

    // A pure filter (no score) on the same predicate must keep returning
    // zero rows as well — regression guard against accidentally turning the
    // fix into "always match" or "always nullopt".
    qt_filter_only_no_score """
        select id
        from test_bm25_score_variant_boolean_subcolumn
        where v["c"] match "abc"
        order by id
    """

    // Sub-column that actually has fulltext content in some segment must
    // still score correctly after the fix (no over-zealous skip).
    qt_score_match_string_subcolumn """
        select id, score() > 0 as has_score
        from test_bm25_score_variant_boolean_subcolumn
        where v["a"] match "abc"
        order by id
    """
}
