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

suite("test_bm25_score_variant", "p0") {
    if (isCloudMode()) {
        return
    }

    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_match_without_inverted_index = false """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """

    // A1: field_pattern exact name (MATCH_NAME)
    try {
        sql "DROP TABLE IF EXISTS test_bm25_score_variant_a1"
        sql """
            CREATE TABLE test_bm25_score_variant_a1 (
                id INT,
                v variant<
                    MATCH_NAME 'host' : text,
                    PROPERTIES("variant_max_subcolumns_count"="0")
                >,
                INDEX idx_v_host (v) USING INVERTED PROPERTIES(
                    "parser"="english",
                    "support_phrase"="true",
                    "field_pattern"="host"
                )
            ) ENGINE=OLAP DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            )
        """
        sql """ insert into test_bm25_score_variant_a1 values
                (1, '{"host":"alpha database server"}'),
                (2, '{"host":"beta server cluster"}'),
                (3, '{"other":"alpha"}')
        """
        sql " sync "

        def res = sql """
            select id, score() as score
            from test_bm25_score_variant_a1
            where cast(v["host"] as string) match_phrase "alpha"
            order by score() desc
            limit 10
        """
        assertEquals(1, res.size())
        assertEquals(1, res[0][0] as int)
        assertTrue(Double.parseDouble(res[0][1].toString()) > 0.0)
    } finally {
    }

    // C: plain parent inverted index (baseline; not the fallback path)
    try {
        sql "DROP TABLE IF EXISTS test_bm25_score_variant_c"
        sql """
            CREATE TABLE test_bm25_score_variant_c (
                id INT,
                v VARIANT,
                INDEX idx_v_plain (v) USING INVERTED PROPERTIES(
                    "parser"="english",
                    "support_phrase"="true"
                )
            ) ENGINE=OLAP DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            )
        """
        sql """ insert into test_bm25_score_variant_c values
                (1, '{"note":"latency spike at noon"}'),
                (2, '{"note":"all green"}')
        """
        sql " sync "

        def res = sql """
            select id, score() as score
            from test_bm25_score_variant_c
            where cast(v["note"] as string) match_phrase "latency"
            order by score() desc
            limit 10
        """
        assertEquals(1, res.size())
        assertEquals(1, res[0][0] as int)
        assertTrue(Double.parseDouble(res[0][1].toString()) > 0.0)
    } finally {
    }
}
