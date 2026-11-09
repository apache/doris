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

// Preserve complete bounds through multiple projections when the merged list fits exactly K.
suite("test_ivm_agg_minmax_topk_8") {
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_dml_materialized_view_rewrite = false;"""
    sql """drop materialized view if exists ivm_topk_8_mv"""
    sql """drop table if exists ivm_topk_8_t"""

    sql """
        CREATE TABLE ivm_topk_8_t (
            id BIGINT NOT NULL,
            score BIGINT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ivm_topk_8_t VALUES (1, 10)"""
    sql """
        CREATE MATERIALIZED VIEW ivm_topk_8_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "ivm_minmax_top_k" = "2"
        )
        AS SELECT MIN(score) + 0 AS mn, MAX(score) AS mx, COUNT(*) AS n, SUM(id) AS total FROM ivm_topk_8_t
    """

    refreshIvm("ivm_topk_8_mv", """REFRESH MATERIALIZED VIEW ivm_topk_8_mv COMPLETE""")
    waitingMTMVTaskFinishedByMvName("ivm_topk_8_mv")
    sql """INSERT INTO ivm_topk_8_t VALUES (2, 30)"""
    refreshIvm("ivm_topk_8_mv", """REFRESH MATERIALIZED VIEW ivm_topk_8_mv INCREMENTAL""")
    waitingMTMVTaskFinishedByMvName("ivm_topk_8_mv")
    ivm_hidden_qt("order_exact_k", """SELECT * FROM ivm_topk_8_mv""")
    sql """DELETE FROM ivm_topk_8_t WHERE id = 2"""
    sql """INSERT INTO ivm_topk_8_t VALUES (3, 40)"""
    refreshIvm("ivm_topk_8_mv", """REFRESH MATERIALIZED VIEW ivm_topk_8_mv INCREMENTAL""")
    waitingMTMVTaskFinishedByMvName("ivm_topk_8_mv")
    ivm_hidden_qt("order_replace", """SELECT * FROM ivm_topk_8_mv""")
    sql """DELETE FROM ivm_topk_8_t WHERE id = 1"""
    refreshIvm("ivm_topk_8_mv", """REFRESH MATERIALIZED VIEW ivm_topk_8_mv INCREMENTAL""")
    waitingMTMVTaskFinishedByMvName("ivm_topk_8_mv")
    order_qt_final """SELECT mn, mx, n, total FROM ivm_topk_8_mv"""
    order_qt_source """SELECT MIN(score), MAX(score), COUNT(*), SUM(id) FROM ivm_topk_8_t"""
    order_qt_nested_lambda """SELECT id,
        array_map(z -> array_map(y -> y, z), array(array(score))) AS a,
        array_map(z -> array_map(y -> y, z), array(array(score))) AS b
        FROM ivm_topk_8_t"""
}
