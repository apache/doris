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

suite("test_ivm_bitmap_runtime_fallback", "nonConcurrent") {
    sql """drop materialized view if exists ivm_bm_fb_mv"""
    sql """drop table if exists ivm_bm_fb_t"""

    sql """
        CREATE TABLE ivm_bm_fb_t (
            id BIGINT NOT NULL,
            category VARCHAR(16) NOT NULL,
            user_id BIGINT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ivm_bm_fb_t VALUES
        (1, 'a', 1), (2, 'a', 2), (3, 'a', 3)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_bm_fb_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        KEY(category)
        DISTRIBUTED BY HASH(category) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT category, BITMAP_UNION(TO_BITMAP(user_id)) AS users
        FROM ivm_bm_fb_t
        GROUP BY category
    """

    def latestTask = {
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT Status, RefreshMode, IvmFallbackReason, ErrorMsg
                FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}'
                  AND MvName = 'ivm_bm_fb_mv'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0]
    }

    sql """REFRESH MATERIALIZED VIEW ivm_bm_fb_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_bm_fb_mv")
    order_qt_bitmap_initial """
        SELECT category, bitmap_to_string(users)
        FROM ivm_bm_fb_mv
        ORDER BY category
    """

    sql """DELETE FROM ivm_bm_fb_t WHERE id = 1"""
    Thread.sleep(1000)

    sql """REFRESH MATERIALIZED VIEW ivm_bm_fb_mv INCREMENTAL"""
    def strictTask = latestTask()
    assertEquals("FAILED", strictTask.Status.toString())
    assertEquals("BITMAP_AGG_DELETE", strictTask.IvmFallbackReason.toString())
    assertTrue(strictTask.ErrorMsg.toString().contains("deleted row affects BITMAP aggregate"))
    order_qt_bitmap_after_strict_failure """
        SELECT category, bitmap_to_string(users)
        FROM ivm_bm_fb_mv
        ORDER BY category
    """

    sql """REFRESH MATERIALIZED VIEW ivm_bm_fb_mv INCREMENTAL FALLBACK"""
    def fallbackTask = latestTask()
    assertEquals("SUCCESS", fallbackTask.Status.toString())
    assertEquals("COMPLETE", fallbackTask.RefreshMode.toString())
    assertEquals("BITMAP_AGG_DELETE", fallbackTask.IvmFallbackReason.toString())
    order_qt_bitmap_after_fallback """
        SELECT category, bitmap_to_string(users)
        FROM ivm_bm_fb_mv
        ORDER BY category
    """
    order_qt_bitmap_source_after_fallback """
        SELECT category, bitmap_to_string(bitmap_union(to_bitmap(user_id)))
        FROM ivm_bm_fb_t
        GROUP BY category
        ORDER BY category
    """
}
