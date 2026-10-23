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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

// Multi-group behaviour: a group with exactly K distinct values and a group with fewer are both
// complete; a new all-NULL group starts empty; one group exhausting its range rebuilds the whole
// MV, and the rebuilt state keeps serving incremental refreshes.
suite("test_ivm_agg_minmax_topk_4") {
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_dml_materialized_view_rewrite = false;"""
    sql """drop materialized view if exists ivm_topk_4_mv"""
    sql """drop table if exists ivm_topk_4_t"""

    sql """
        CREATE TABLE ivm_topk_4_t (
            id BIGINT NOT NULL,
            category VARCHAR(16) NOT NULL,
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
    sql """INSERT INTO ivm_topk_4_t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 5), (4, 'b', NULL)"""
    sql """
        CREATE MATERIALIZED VIEW ivm_topk_4_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        KEY(category)
        DISTRIBUTED BY HASH(category) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "ivm_minmax_top_k" = "2"
        )
        AS SELECT category, MIN(score) AS min_score, MAX(score) AS max_score, COUNT(score) AS cnt
        FROM ivm_topk_4_t
        GROUP BY category
    """

    def latestTask = {
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT Status, IvmFallbackReason FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_topk_4_mv'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0]
    }

    sql """REFRESH MATERIALIZED VIEW ivm_topk_4_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_4_mv")
    // Group a has exactly K distinct values, group b one value plus a NULL row: both are complete,
    // so their bound stays NULL.
    order_qt_initial """SELECT category, min_score, max_score, cnt FROM ivm_topk_4_mv ORDER BY category"""
    ivm_hidden_qt("state_initial", """SELECT * FROM ivm_topk_4_mv ORDER BY 1, 2""")

    // A complete group accepts a new value while it still fits in K.
    sql """INSERT INTO ivm_topk_4_t VALUES (5, 'a', 30)"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_4_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_4_mv")
    order_qt_after_insert """SELECT category, min_score, max_score, cnt FROM ivm_topk_4_mv ORDER BY category"""
    ivm_hidden_qt("state_after_insert", """SELECT * FROM ivm_topk_4_mv ORDER BY 1, 2""")

    // A new all-NULL group: MIN/MAX are NULL and the aggregate state is an empty list.
    sql """INSERT INTO ivm_topk_4_t VALUES (6, 'c', NULL)"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_4_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_4_mv")
    order_qt_after_null_group """SELECT category, min_score, max_score, cnt FROM ivm_topk_4_mv ORDER BY category"""
    ivm_hidden_qt("state_after_null_group", """SELECT * FROM ivm_topk_4_mv ORDER BY 1, 2""")

    // Removing the NULL row does not change the group's count of non-NULL values.
    sql """DELETE FROM ivm_topk_4_t WHERE id = 4"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_4_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_4_mv")
    order_qt_after_null_delete """SELECT category, min_score, max_score, cnt FROM ivm_topk_4_mv ORDER BY category"""

    // Group a exhausts its known range while b and c are fine: fallback is whole-MV.
    sql """DELETE FROM ivm_topk_4_t WHERE id IN (1, 2)"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_4_mv INCREMENTAL"""
    def exhausted = latestTask()
    assertEquals("FAILED", exhausted.Status.toString())
    assertEquals("MIN_MAX_BOUNDARY_HIT", exhausted.IvmFallbackReason.toString())
    order_qt_after_exhaustion """SELECT category, min_score, max_score, cnt FROM ivm_topk_4_mv ORDER BY category"""

    sql """REFRESH MATERIALIZED VIEW ivm_topk_4_mv INCREMENTAL FALLBACK"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_4_mv")
    order_qt_after_fallback """SELECT category, min_score, max_score, cnt FROM ivm_topk_4_mv ORDER BY category"""
    ivm_hidden_qt("state_after_fallback", """SELECT * FROM ivm_topk_4_mv ORDER BY 1, 2""")

    // The rebuilt state continues incrementally: a smaller value enters group b.
    sql """INSERT INTO ivm_topk_4_t VALUES (7, 'b', 1)"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_4_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_4_mv")
    order_qt_after_rebuild_insert """SELECT category, min_score, max_score, cnt FROM ivm_topk_4_mv ORDER BY category"""
    ivm_hidden_qt("state_after_rebuild_insert", """SELECT * FROM ivm_topk_4_mv ORDER BY 1, 2""")
}
