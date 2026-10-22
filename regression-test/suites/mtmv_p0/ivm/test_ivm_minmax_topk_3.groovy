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

// K = 1 on a scalar aggregate (no GROUP BY): the state is the extreme plus its count, the logical
// equivalent of the old min-count state. Deleting a duplicate extreme is incremental; deleting the
// last one exhausts the state.
import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_ivm_minmax_topk_3") {
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_dml_materialized_view_rewrite = false;"""
    sql """drop materialized view if exists ivm_topk_3_mv"""
    sql """drop table if exists ivm_topk_3_t"""

    sql """
        CREATE TABLE ivm_topk_3_t (
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
    sql """INSERT INTO ivm_topk_3_t VALUES (1, 10), (2, 10), (3, 20)"""
    sql """
        CREATE MATERIALIZED VIEW ivm_topk_3_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "ivm_minmax_top_k" = "1"
        )
        AS SELECT MIN(score) AS mn FROM ivm_topk_3_t
    """

    def latestTask = {
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT Status, IvmFallbackReason FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_topk_3_mv'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult.Status.toString() != 'PENDING'
                    && taskResult.Status.toString() != 'RUNNING'
        })
        return taskResult[0]
    }

    sql """REFRESH MATERIALIZED VIEW ivm_topk_3_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_3_mv")
    order_qt_initial """SELECT mn FROM ivm_topk_3_mv"""
    ivm_hidden_qt("state_initial", """SELECT * FROM ivm_topk_3_mv""")

    // Deleting one of the duplicate extremes only decrements its count.
    sql """DELETE FROM ivm_topk_3_t WHERE id = 1"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_3_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_3_mv")
    def afterDuplicate = latestTask()
    assertEquals("SUCCESS", afterDuplicate.Status.toString())
    order_qt_after_duplicate """SELECT mn FROM ivm_topk_3_mv"""
    ivm_hidden_qt("state_after_duplicate", """SELECT * FROM ivm_topk_3_mv""")

    // Deleting the last row at the extreme exhausts K = 1 and needs a complete rebuild.
    sql """DELETE FROM ivm_topk_3_t WHERE id = 2"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_3_mv INCREMENTAL"""
    def exhausted = latestTask()
    assertEquals("FAILED", exhausted.Status.toString())
    assertEquals("MIN_MAX_BOUNDARY_HIT", exhausted.IvmFallbackReason.toString())
    sql """REFRESH MATERIALIZED VIEW ivm_topk_3_mv INCREMENTAL FALLBACK"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_3_mv")
    order_qt_after_fallback """SELECT mn FROM ivm_topk_3_mv"""
    ivm_hidden_qt("state_after_fallback", """SELECT * FROM ivm_topk_3_mv""")
}
