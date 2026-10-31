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


suite("test_ivm_minmax_count") {
    // The base-table queries below assert the same rows as the MV, so transparent rewrite must be off.
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_dml_materialized_view_rewrite = false;"""
    sql """drop materialized view if exists ivm_mm_count_mv"""
    sql """drop table if exists ivm_mm_count_t"""

    sql """
        CREATE TABLE ivm_mm_count_t (
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

    // category a: min=10 twice, max=30 once
    // category b: min=5 three times, max=5
    sql """INSERT INTO ivm_mm_count_t VALUES
        (1, 'a', 10), (2, 'a', 10), (3, 'a', 30),
        (4, 'b', 5),  (5, 'b', 5),  (6, 'b', 5)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_mm_count_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        KEY(category)
        DISTRIBUTED BY HASH(category) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "ivm_minmax_top_k" = "1")
        AS SELECT category, MIN(score) AS min_score, MAX(score) AS max_score, COUNT(*) AS row_count
        FROM ivm_mm_count_t
        GROUP BY category
    """

    def assertNoFallback = { task ->
        def reason = task.IvmFallbackReason
        assertTrue(reason == null || reason.toString() == "\\N",
                "unexpected IVM fallback: ${reason}")
    }

    // A task that just finished is briefly absent from tasks(): the job removes it from its
    // running list before the MV history publishes it, so the newest row can temporarily be
    // the previous refresh. Require a different TaskId before trusting a terminal row.
    String lastTaskId = null
    sql """REFRESH MATERIALIZED VIEW ivm_mm_count_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_mm_count_mv")
    lastTaskId = sql_return_maparray("""
        SELECT TaskId
        FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}'
          AND MvName = 'ivm_mm_count_mv'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """)[0].TaskId.toString()
    order_qt_initial """
        SELECT category, min_score, max_score, row_count
        FROM ivm_mm_count_mv
    """

    // Delete one of the two rows that hold the minimum of category a. The minimum survives,
    // so the hidden min-count state keeps the refresh incremental.
    sql """DELETE FROM ivm_mm_count_t WHERE id = 1"""
    Thread.sleep(1000)
    sql """REFRESH MATERIALIZED VIEW ivm_mm_count_mv INCREMENTAL"""
    def duplicateMinTask = latestIvmTask("ivm_mm_count_mv")
    assertEquals("SUCCESS", duplicateMinTask.Status.toString())
    assertNoFallback(duplicateMinTask)
    order_qt_after_duplicate_min_delete """
        SELECT category, min_score, max_score, row_count
        FROM ivm_mm_count_mv
    """

    // Delete one of the three rows that hold the maximum of category b. The maximum survives.
    sql """DELETE FROM ivm_mm_count_t WHERE id = 4"""
    Thread.sleep(1000)
    sql """REFRESH MATERIALIZED VIEW ivm_mm_count_mv INCREMENTAL"""
    def duplicateMaxTask = latestIvmTask("ivm_mm_count_mv")
    assertEquals("SUCCESS", duplicateMaxTask.Status.toString())
    assertNoFallback(duplicateMaxTask)
    order_qt_after_duplicate_max_delete """
        SELECT category, min_score, max_score, row_count
        FROM ivm_mm_count_mv
    """

    // Delete the last row holding category a's minimum. Its count reaches zero and no smaller
    // value was inserted, so the refresh still falls back to a complete refresh.
    sql """DELETE FROM ivm_mm_count_t WHERE id = 2"""
    Thread.sleep(1000)
    sql """REFRESH MATERIALIZED VIEW ivm_mm_count_mv INCREMENTAL FALLBACK"""
    def lastMinTask = latestIvmTask("ivm_mm_count_mv")
    assertEquals("SUCCESS", lastMinTask.Status.toString())
    assertEquals("COMPLETE", lastMinTask.RefreshMode.toString())
    assertEquals("MIN_MAX_BOUNDARY_HIT", lastMinTask.IvmFallbackReason.toString())
    order_qt_after_last_min_delete """
        SELECT category, min_score, max_score, row_count
        FROM ivm_mm_count_mv
    """

    // A smaller inserted value becomes the new minimum without a complete refresh.
    sql """INSERT INTO ivm_mm_count_t VALUES (7, 'a', 7), (8, 'a', 7)"""
    Thread.sleep(1000)
    sql """REFRESH MATERIALIZED VIEW ivm_mm_count_mv INCREMENTAL"""
    def smallerInsertTask = latestIvmTask("ivm_mm_count_mv")
    assertEquals("SUCCESS", smallerInsertTask.Status.toString())
    assertNoFallback(smallerInsertTask)
    order_qt_after_smaller_insert """
        SELECT category, min_score, max_score, row_count
        FROM ivm_mm_count_mv
    """

    // The source query must agree with the MV after every step.
    order_qt_source """
        SELECT category, MIN(score), MAX(score), COUNT(*)
        FROM ivm_mm_count_t
        GROUP BY category
    """
}
