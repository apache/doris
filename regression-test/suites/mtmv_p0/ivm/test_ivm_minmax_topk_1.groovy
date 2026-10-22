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

// Core top-K state transitions with an explicit capacity: truncation, deleting duplicate and last
// extremes, deleting above the bound, exhausting the known range, and changing K with ALTER.
// `SELECT *` with hidden columns prints the state struct of every step; the sequence column differs
// between cloud and shared-nothing deployments and is masked.
suite("test_ivm_minmax_topk_1") {
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_dml_materialized_view_rewrite = false;"""
    sql """drop materialized view if exists ivm_topk_1_mv"""
    sql """drop table if exists ivm_topk_1_t"""

    sql """
        CREATE TABLE ivm_topk_1_t (
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
    sql """INSERT INTO ivm_topk_1_t VALUES
        (1, 'a', 10), (7, 'a', 10), (2, 'a', 20), (3, 'a', 30), (4, 'a', 40)"""
    sql """
        CREATE MATERIALIZED VIEW ivm_topk_1_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        KEY(category)
        DISTRIBUTED BY HASH(category) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "ivm_minmax_top_k" = "2"
        )
        AS SELECT category, MIN(score) AS min_score, MAX(score) AS max_score
        FROM ivm_topk_1_t
        GROUP BY category
    """

    def latestTask = {
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT Status, RefreshMode, IvmFallbackReason FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_topk_1_mv'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0]
    }

    // The resolved capacity is persisted at CREATE. Assert the one property instead of pinning the
    // whole DDL, whose unrelated defaults would make the golden churn.
    def createDdl = sql """SHOW CREATE MATERIALIZED VIEW ivm_topk_1_mv"""
    assertTrue(createDdl.toString().contains('"ivm_minmax_top_k" = "2"'))
    // DESC with hidden columns pins the state column type next to the per-step state values below.
    sql """SET show_hidden_columns = true;"""
    qt_desc_hidden """desc ivm_topk_1_mv"""
    sql """SET show_hidden_columns = false;"""

    sql """REFRESH MATERIALIZED VIEW ivm_topk_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_1_mv")
    order_qt_initial """SELECT category, min_score, max_score FROM ivm_topk_1_mv ORDER BY category"""
    ivm_hidden_qt("state_initial", """SELECT * FROM ivm_topk_1_mv ORDER BY 1, 2""")

    // A smaller value enters the list and pushes the bound down.
    sql """INSERT INTO ivm_topk_1_t VALUES (8, 'a', 5)"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_1_mv")
    assertEquals("SUCCESS", latestTask().Status.toString())
    order_qt_after_insert """SELECT category, min_score, max_score FROM ivm_topk_1_mv ORDER BY category"""
    ivm_hidden_qt("state_after_insert", """SELECT * FROM ivm_topk_1_mv ORDER BY 1, 2""")

    // Deleting one of several rows at the extreme only decrements its count.
    sql """DELETE FROM ivm_topk_1_t WHERE id = 1"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_1_mv")
    assertEquals("SUCCESS", latestTask().Status.toString())
    order_qt_after_delete_one """SELECT category, min_score, max_score FROM ivm_topk_1_mv ORDER BY category"""
    ivm_hidden_qt("state_after_delete_one", """SELECT * FROM ivm_topk_1_mv ORDER BY 1, 2""")

    // Deleting the last row at the extreme retires the value from the list.
    sql """DELETE FROM ivm_topk_1_t WHERE id = 7"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_1_mv")
    assertEquals("SUCCESS", latestTask().Status.toString())
    order_qt_after_delete_extreme """SELECT category, min_score, max_score FROM ivm_topk_1_mv ORDER BY category"""
    ivm_hidden_qt("state_after_delete_extreme", """SELECT * FROM ivm_topk_1_mv ORDER BY 1, 2""")

    // Deleting a value above the trusted bound does not move the extreme.
    sql """DELETE FROM ivm_topk_1_t WHERE id = 2"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_1_mv")
    assertEquals("SUCCESS", latestTask().Status.toString())
    order_qt_after_delete_above_bound """SELECT category, min_score, max_score FROM ivm_topk_1_mv ORDER BY category"""
    ivm_hidden_qt("state_after_delete_above_bound", """SELECT * FROM ivm_topk_1_mv ORDER BY 1, 2""")

    // Deleting the only cached value exhausts the known range while rows remain: strict refresh
    // fails, and only an explicit FALLBACK rebuilds the MV.
    sql """DELETE FROM ivm_topk_1_t WHERE id = 8"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_1_mv INCREMENTAL"""
    def exhaustedTask = latestTask()
    assertEquals("FAILED", exhaustedTask.Status.toString())
    assertEquals("MIN_MAX_BOUNDARY_HIT", exhaustedTask.IvmFallbackReason.toString())
    order_qt_after_exhaustion """SELECT category, min_score, max_score FROM ivm_topk_1_mv ORDER BY category"""

    sql """REFRESH MATERIALIZED VIEW ivm_topk_1_mv INCREMENTAL FALLBACK"""
    def fallbackTask = latestTask()
    assertEquals("SUCCESS", fallbackTask.Status.toString())
    assertEquals("COMPLETE", fallbackTask.RefreshMode.toString())
    order_qt_after_fallback """SELECT category, min_score, max_score FROM ivm_topk_1_mv ORDER BY category"""
    ivm_hidden_qt("state_after_fallback", """SELECT * FROM ivm_topk_1_mv ORDER BY 1, 2""")

    // A different capacity cannot be interpreted against the old state: ALTER rebuilds the MV.
    sql """ALTER MATERIALIZED VIEW ivm_topk_1_mv SET ("ivm_minmax_top_k" = "4")"""
    def alteredDdl = sql """SHOW CREATE MATERIALIZED VIEW ivm_topk_1_mv"""
    assertTrue(alteredDdl.toString().contains('"ivm_minmax_top_k" = "4"'))
    sql """REFRESH MATERIALIZED VIEW ivm_topk_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_1_mv")
    ivm_hidden_qt("state_after_alter", """SELECT * FROM ivm_topk_1_mv ORDER BY 1, 2""")
}
