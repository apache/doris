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

// String values: the top-K list must order and deduplicate strings exactly like MIN/MAX, so the
// visible extreme always equals the first element of the printed state.
suite("test_ivm_minmax_topk_7") {
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_dml_materialized_view_rewrite = false;"""
    sql """drop materialized view if exists ivm_topk_7_mv"""
    sql """drop table if exists ivm_topk_7_t"""

    sql """
        CREATE TABLE ivm_topk_7_t (
            id BIGINT NOT NULL,
            name VARCHAR(16)
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
    sql """INSERT INTO ivm_topk_7_t VALUES (1, 'A'), (2, 'a'), (3, 'b')"""
    sql """
        CREATE MATERIALIZED VIEW ivm_topk_7_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "ivm_minmax_top_k" = "2"
        )
        AS SELECT MIN(name) AS mn, MAX(name) AS mx FROM ivm_topk_7_t
    """

    def latestTask = {
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT Status, IvmFallbackReason FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_topk_7_mv'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0]
    }

    sql """REFRESH MATERIALIZED VIEW ivm_topk_7_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_topk_7_mv")
    order_qt_initial """SELECT mn, mx FROM ivm_topk_7_mv"""
    // Three distinct values with K = 2: both lists are truncated, the MIN bound is 'a' and the
    // MAX bound is 'a' as well (the second largest value).
    ivm_hidden_qt("state_initial", """SELECT * FROM ivm_topk_7_mv""")

    // Deleting the smallest value retires it from the MIN list and keeps MAX unchanged.
    sql """DELETE FROM ivm_topk_7_t WHERE id = 1"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_7_mv INCREMENTAL"""
    def afterDelete = latestTask()
    assertEquals("SUCCESS", afterDelete.Status.toString())
    order_qt_after_delete """SELECT mn, mx FROM ivm_topk_7_mv"""
    ivm_hidden_qt("state_after_delete", """SELECT * FROM ivm_topk_7_mv""")

    // Inserting a value between the remaining ones only changes the MAX-side candidate list.
    sql """INSERT INTO ivm_topk_7_t VALUES (4, 'aa')"""
    sql """REFRESH MATERIALIZED VIEW ivm_topk_7_mv INCREMENTAL"""
    def afterInsert = latestTask()
    assertEquals("SUCCESS", afterInsert.Status.toString())
    order_qt_after_insert """SELECT mn, mx FROM ivm_topk_7_mv"""
    ivm_hidden_qt("state_after_insert", """SELECT * FROM ivm_topk_7_mv""")
}
