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
//
// This regression uses plain `sql` + assertions (rather than qt_<name>) so
// it doesn't depend on a hand-maintained .out file: the property being
// tested is "BE survives the query"; the exact ordering of any matching
// row is not load-bearing.
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

    // Core regression: before the fix this aborts the BE. After the fix it
    // must return cleanly and produce zero rows (no segment has v.c indexed
    // as a string term matching "abc"). The FE score() TopN push-down
    // requires exactly one ordering expression, so order by score() alone.
    def scoredRows = sql """
        select id, score()
        from test_bm25_score_variant_boolean_subcolumn
        where v["c"] match "abc"
        order by score()
        limit 10
    """
    assertEquals(0, scoredRows.size())

    // A pure filter (no score) on the same predicate must also keep
    // returning zero rows — guards against accidentally turning the fix
    // into "always match" or "always nullopt".
    def filterRows = sql """
        select id
        from test_bm25_score_variant_boolean_subcolumn
        where v["c"] match "abc"
        order by id
    """
    assertEquals(0, filterRows.size())

    // Sub-column that actually has fulltext content in some segment must
    // still score correctly after the fix (no over-zealous skip).
    // v.a = "abc" in row 1, so we expect exactly one matching row.
    def hitRows = sql """
        select id
        from test_bm25_score_variant_boolean_subcolumn
        where v["a"] match "abc"
        order by id
    """
    assertEquals(1, hitRows.size())
    assertEquals(1, (hitRows[0][0] as int))

    // And score() must finish and produce a (positive) value for that row,
    // confirming the BM25 stats path still works for the string sub-column.
    def hitScored = sql """
        select id, score()
        from test_bm25_score_variant_boolean_subcolumn
        where v["a"] match "abc"
        order by id
    """
    assertEquals(1, hitScored.size())
    assertEquals(1, (hitScored[0][0] as int))
    assertTrue((hitScored[0][1] as double) > 0.0d)
}
