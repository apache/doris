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

// A numeric (BKD) leaf that matches more than inverted_index_skip_threshold percent of a
// segment used to return INVERTED_INDEX_BYPASS. SEARCH has no row-by-row path, so the
// query failed with "SearchExpr should not be executed without inverted index". SEARCH must
// always use the index.
suite("test_search_bkd_skip_threshold", "p0") {
    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_inverted_index_query_cache = true """
    sql """ set enable_sql_cache = false """
    sql """ set inverted_index_skip_threshold = 50 """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """
    // Keep both v.n and v.p as indexed subcolumns; a fuzzy budget of 1 would push one to sparse.
    sql """ set default_variant_max_subcolumns_count = 2048 """

    sql "DROP TABLE IF EXISTS test_search_bkd_skip_threshold_t"
    sql """
        CREATE TABLE test_search_bkd_skip_threshold_t (
            `id` INT NOT NULL,
            `k` INT NULL,
            `v` variant NULL,
            INDEX idx_k (k) USING INVERTED,
            INDEX idx_v (v) USING INVERTED PROPERTIES(
                "parser" = "unicode", "lower_case" = "true", "support_phrase" = "true")
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true"
        )
    """

    // 7 of 10 rows have n = 5 and k = 5, so a BKD lookup for 5 hits 70% of the segment.
    sql """INSERT INTO test_search_bkd_skip_threshold_t VALUES
        (1, 5, '{"n": 5, "p": "software engineer"}'),
        (2, 5, '{"n": 5, "p": "sales"}'),
        (3, 5, '{"n": 5, "p": "software developer"}'),
        (4, 5, '{"n": 5, "p": "sales"}'),
        (5, 5, '{"n": 5, "p": "software engineer"}'),
        (6, 5, '{"n": 5, "p": "sales"}'),
        (7, 5, '{"n": 5, "p": "software tester"}'),
        (8, 6, '{"n": 6, "p": "software engineer"}'),
        (9, 6, '{"n": 6, "p": "sales"}'),
        (10, 7, '{"n": 7, "p": "software engineer"}')
    """

    // The first lookup of a value can pass on the BKD estimate. Warm both numeric leaves so the
    // exact hit count is in the inverted index query cache; every check below then runs its
    // SEARCH expression for the first time and sees that count, which is over the threshold.
    sql "SELECT count(*) FROM test_search_bkd_skip_threshold_t WHERE search('v.n:5')"
    sql "SELECT count(*) FROM test_search_bkd_skip_threshold_t WHERE search('k:5')"

    qt_dense_variant """SELECT count(*) FROM test_search_bkd_skip_threshold_t WHERE search('v.n:5')"""
    qt_dense_column """SELECT count(*) FROM test_search_bkd_skip_threshold_t WHERE search('k:5')"""
    qt_dense_and_text """SELECT count(*) FROM test_search_bkd_skip_threshold_t
                         WHERE search('v.n:5 AND v.p:software')"""
    qt_dense_or """SELECT count(*) FROM test_search_bkd_skip_threshold_t WHERE search('v.n:5 OR v.n:6')"""
    qt_sparse_variant """SELECT count(*) FROM test_search_bkd_skip_threshold_t WHERE search('v.n:6')"""
    qt_sparse_column """SELECT count(*) FROM test_search_bkd_skip_threshold_t WHERE search('k:6')"""
    qt_dense_topn """SELECT id FROM test_search_bkd_skip_threshold_t
                     WHERE search('v.n:5') ORDER BY id DESC LIMIT 2"""
    qt_dense_standard_mode """SELECT count(*) FROM test_search_bkd_skip_threshold_t
                              WHERE search('v.n:5 AND v.p:software', '{"mode":"standard"}')"""
}
