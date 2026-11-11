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
// query failed with "SEARCH cannot fall back to row execution". SEARCH must always use
// the index.
suite("test_search_bkd_skip_threshold", "p0") {
    def tableName = "test_search_bkd_skip_threshold"

    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_inverted_index_query_cache = true """
    sql """ set inverted_index_skip_threshold = 50 """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
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
    sql """INSERT INTO ${tableName} VALUES
        (1, 5, parse_to_variant('{"n": 5, "p": "software engineer"}')),
        (2, 5, parse_to_variant('{"n": 5, "p": "sales"}')),
        (3, 5, parse_to_variant('{"n": 5, "p": "software developer"}')),
        (4, 5, parse_to_variant('{"n": 5, "p": "sales"}')),
        (5, 5, parse_to_variant('{"n": 5, "p": "software engineer"}')),
        (6, 5, parse_to_variant('{"n": 5, "p": "sales"}')),
        (7, 5, parse_to_variant('{"n": 5, "p": "software tester"}')),
        (8, 6, parse_to_variant('{"n": 6, "p": "software engineer"}')),
        (9, 6, parse_to_variant('{"n": 6, "p": "sales"}')),
        (10, 7, parse_to_variant('{"n": 7, "p": "software engineer"}'))
    """

    def checks = [
        ["v.n:5", 7],
        ["k:5", 7],
        ["v.n:5 AND v.p:software", 4],
        ["v.n:5 OR v.n:6", 9],
        ["v.n:6", 2],
        ["k:6", 2],
    ]
    // Run every query several times: the first run can pass on the BKD estimate and the
    // later ones hit the query cache, where the exact hit count is over the threshold.
    for (int round = 0; round < 3; round++) {
        for (def c : checks) {
            def res = sql "SELECT count(*) FROM ${tableName} WHERE search('${c[0]}')"
            assertEquals(c[1], res[0][0] as int, "search('${c[0]}') round ${round}")
        }
        def top = sql """SELECT id FROM ${tableName} WHERE search('v.n:5') ORDER BY id DESC LIMIT 2"""
        assertEquals([[7], [6]], top.collect { [it[0] as int] })
        def mode = sql """SELECT count(*) FROM ${tableName}
                          WHERE search('v.n:5 AND v.p:software', '{"mode":"standard"}')"""
        assertEquals(4, mode[0][0] as int)
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
}
