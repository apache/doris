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


// Delta shape edge cases: a single batch inserting more than K distinct values truncates the
// insert-side list and collapses the trusted bound, and an UPDATE moves a value out of the MAX side
// and into the MIN side at once.
suite("test_ivm_agg_minmax_topk_6") {
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_dml_materialized_view_rewrite = false;"""
    sql """drop materialized view if exists ivm_topk_6_mv"""
    sql """drop table if exists ivm_topk_6_t"""

    sql """
        CREATE TABLE ivm_topk_6_t (
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
    sql """INSERT INTO ivm_topk_6_t VALUES (1, 10), (2, 20), (3, 30)"""
    sql """
        CREATE MATERIALIZED VIEW ivm_topk_6_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "ivm_minmax_top_k" = "2"
        )
        AS SELECT MIN(score) AS mn, MAX(score) AS mx FROM ivm_topk_6_t
    """

    sql """REFRESH MATERIALIZED VIEW ivm_topk_6_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_6_mv")
    order_qt_initial """SELECT mn, mx FROM ivm_topk_6_mv"""
    ivm_hidden_qt("order_state_initial", """SELECT * FROM ivm_topk_6_mv""")

    // Three distinct values in one batch with K = 2: the insert-side list is truncated at 6, so the
    // trusted bound collapses to 6 and 7 is never trusted.
    sql """INSERT INTO ivm_topk_6_t VALUES (4, 5), (5, 6), (6, 7)"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_6_mv INCREMENTAL"""
    def afterTruncatingInsert = latestIvmTask("ivm_topk_6_mv")
    assertEquals("SUCCESS", afterTruncatingInsert.Status.toString())
    order_qt_after_truncating_insert """SELECT mn, mx FROM ivm_topk_6_mv"""
    ivm_hidden_qt("order_state_after_truncating_insert", """SELECT * FROM ivm_topk_6_mv""")

    // One UPDATE emits a delete of 30 and an insert of 4: the MAX side loses its extreme and the
    // MIN side gains a smaller value in the same batch.
    sql """UPDATE ivm_topk_6_t SET score = 4 WHERE id = 3"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_6_mv INCREMENTAL"""
    def afterUpdate = latestIvmTask("ivm_topk_6_mv")
    assertEquals("SUCCESS", afterUpdate.Status.toString())
    order_qt_after_update """SELECT mn, mx FROM ivm_topk_6_mv"""
    ivm_hidden_qt("order_state_after_update", """SELECT * FROM ivm_topk_6_mv""")
}
