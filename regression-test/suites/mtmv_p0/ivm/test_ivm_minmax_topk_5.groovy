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

// Join delta cancellation: one UPDATE moves rows between groups, so each group sees the same value
// deleted and inserted in the same batch. The delete and insert must cancel instead of consuming the
// top-K slots or triggering a fallback.
suite("test_ivm_minmax_topk_5") {
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_dml_materialized_view_rewrite = false;"""
    sql """drop materialized view if exists ivm_topk_5_mv"""
    sql """drop table if exists ivm_topk_5_t1"""
    sql """drop table if exists ivm_topk_5_t2"""

    sql """
        CREATE TABLE ivm_topk_5_t1 (
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
    sql """
        CREATE TABLE ivm_topk_5_t2 (
            id BIGINT NOT NULL,
            flag VARCHAR(8) NOT NULL
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
    sql """INSERT INTO ivm_topk_5_t1 VALUES (1, 10), (3, 10)"""
    sql """INSERT INTO ivm_topk_5_t2 VALUES (1, 'a'), (3, 'b')"""
    sql """
        CREATE MATERIALIZED VIEW ivm_topk_5_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        KEY(flag)
        DISTRIBUTED BY HASH(flag) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "ivm_minmax_top_k" = "1"
        )
        AS SELECT t2.flag AS flag, MIN(t1.score) AS mn
        FROM ivm_topk_5_t1 t1 JOIN ivm_topk_5_t2 t2 ON t1.id = t2.id
        GROUP BY t2.flag
    """

    def latestTask = {
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT Status, IvmFallbackReason FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_topk_5_mv'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0]
    }

    sql """REFRESH MATERIALIZED VIEW ivm_topk_5_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_5_mv")
    order_qt_initial """SELECT flag, mn FROM ivm_topk_5_mv ORDER BY flag"""
    ivm_hidden_qt("state_initial", """SELECT * FROM ivm_topk_5_mv ORDER BY 1, 2""")

    // Swap the two rows between the groups. Each group gets a delete and an insert of the same value
    // in one batch, so the count must stay 1 and the extreme must survive without a fallback.
    sql """UPDATE ivm_topk_5_t2 SET flag = IF(id = 1, 'b', 'a') WHERE id IN (1, 3)"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_5_mv INCREMENTAL"""
    def afterSwap = latestTask()
    assertEquals("SUCCESS", afterSwap.Status.toString())
    order_qt_after_swap """SELECT flag, mn FROM ivm_topk_5_mv ORDER BY flag"""
    ivm_hidden_qt("state_after_swap", """SELECT * FROM ivm_topk_5_mv ORDER BY 1, 2""")
}
