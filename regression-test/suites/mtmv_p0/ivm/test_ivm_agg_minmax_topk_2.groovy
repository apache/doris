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

// K = 0 (extreme-only): an append-only changelog with no outer join selects it automatically, and
// the state column keeps an empty list plus the current extreme. A retraction breaks the premise
// and falls back; that half runs on a merge-on-write table with an explicit K = 0, because a
// predicate DELETE on a changelog-enabled DUP table cannot produce row-level events.
suite("test_ivm_agg_minmax_topk_2") {
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_dml_materialized_view_rewrite = false;"""
    sql """drop materialized view if exists ivm_topk_2_mv"""
    sql """drop table if exists ivm_topk_2_t"""
    sql """drop materialized view if exists ivm_topk_2_mow_mv"""
    sql """drop table if exists ivm_topk_2_mow_t"""

    def latestTask = { String mvName ->
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT Status, IvmFallbackReason FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mvName}'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0]
    }

    // ---- automatic K = 0 on an append-only base table ----
    sql """
        CREATE TABLE ivm_topk_2_t (
            id BIGINT NOT NULL,
            category VARCHAR(16) NOT NULL,
            score BIGINT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true"
        )
    """
    sql """INSERT INTO ivm_topk_2_t VALUES (1, 'a', 10), (2, 'a', 20)"""
    sql """
        CREATE MATERIALIZED VIEW ivm_topk_2_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        KEY(category)
        DISTRIBUTED BY HASH(category) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT category, MIN(score) AS min_score, MAX(score) AS max_score
        FROM ivm_topk_2_t
        GROUP BY category
    """
    // CREATE persisted the auto-selected capacity. Assert the one property instead of pinning the
    // whole DDL, whose unrelated defaults would make the golden churn.
    def autoDdl = sql """SHOW CREATE MATERIALIZED VIEW ivm_topk_2_mv"""
    assertTrue(autoDdl.toString().contains('"ivm_minmax_top_k" = "0"'))
    sql """SET show_hidden_columns = true;"""
    qt_desc_hidden """desc ivm_topk_2_mv"""
    sql """SET show_hidden_columns = false;"""

    sql """REFRESH MATERIALIZED VIEW ivm_topk_2_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_2_mv")
    order_qt_initial """SELECT category, min_score, max_score FROM ivm_topk_2_mv ORDER BY category"""
    ivm_hidden_qt("state_initial", """SELECT * FROM ivm_topk_2_mv""")

    // Appends stay incremental; only the extreme moves.
    sql """INSERT INTO ivm_topk_2_t VALUES (3, 'a', 5)"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_2_mv INCREMENTAL"""
    def afterInsert = latestTask("ivm_topk_2_mv")
    assertEquals("SUCCESS", afterInsert.Status.toString())
    order_qt_after_insert """SELECT category, min_score, max_score FROM ivm_topk_2_mv ORDER BY category"""
    ivm_hidden_qt("state_after_insert", """SELECT * FROM ivm_topk_2_mv""")

    // A batch whose insert-side extreme is better and appears more than once must take the
    // inserted side's count, not the pre-image's.
    sql """INSERT INTO ivm_topk_2_t VALUES (4, 'a', 1), (5, 'a', 1)"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_2_mv INCREMENTAL"""
    def afterMultiInsert = latestTask("ivm_topk_2_mv")
    assertEquals("SUCCESS", afterMultiInsert.Status.toString())
    order_qt_after_multi_insert """SELECT category, min_score, max_score FROM ivm_topk_2_mv ORDER BY category"""
    ivm_hidden_qt("state_after_multi_insert", """SELECT * FROM ivm_topk_2_mv""")

    // ---- explicit K = 0 on a merge-on-write table: a retraction must fall back ----
    sql """
        CREATE TABLE ivm_topk_2_mow_t (
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
    sql """INSERT INTO ivm_topk_2_mow_t VALUES (1, 10), (2, 20)"""
    sql """
        CREATE MATERIALIZED VIEW ivm_topk_2_mow_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "ivm_minmax_top_k" = "0"
        )
        AS SELECT MIN(score) AS mn FROM ivm_topk_2_mow_t
    """
    def explicitZeroDdl = sql """SHOW CREATE MATERIALIZED VIEW ivm_topk_2_mow_mv"""
    assertTrue(explicitZeroDdl.toString().contains('"ivm_minmax_top_k" = "0"'))
    sql """REFRESH MATERIALIZED VIEW ivm_topk_2_mow_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_2_mow_mv")
    ivm_hidden_qt("mow_state_initial", """SELECT * FROM ivm_topk_2_mow_mv""")

    sql """DELETE FROM ivm_topk_2_mow_t WHERE id = 1"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_2_mow_mv INCREMENTAL"""
    def exhausted = latestTask("ivm_topk_2_mow_mv")
    assertEquals("FAILED", exhausted.Status.toString())
    assertEquals("MIN_MAX_BOUNDARY_HIT", exhausted.IvmFallbackReason.toString())
    order_qt_mow_after_delete """SELECT mn FROM ivm_topk_2_mow_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_topk_2_mow_mv INCREMENTAL FALLBACK"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_2_mow_mv")
    order_qt_mow_after_fallback """SELECT mn FROM ivm_topk_2_mow_mv"""
    ivm_hidden_qt("mow_state_after_fallback", """SELECT * FROM ivm_topk_2_mow_mv""")
}
