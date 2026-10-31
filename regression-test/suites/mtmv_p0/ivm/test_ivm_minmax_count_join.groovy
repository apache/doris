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


suite("test_ivm_minmax_count_join") {
    // The base-table queries below assert the same rows as the MV, so transparent rewrite must be off.
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_dml_materialized_view_rewrite = false;"""
    sql """drop materialized view if exists ivm_mm_join_mv"""
    sql """drop table if exists ivm_mm_join_l"""
    sql """drop table if exists ivm_mm_join_r"""

    sql """
        CREATE TABLE ivm_mm_join_l (
            k BIGINT NOT NULL,
            v BIGINT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql """
        CREATE TABLE ivm_mm_join_r (
            k BIGINT NOT NULL
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """

    // The initial join produces only NULL values, so the MV starts with MIN = NULL and the
    // hidden MIN_COUNT is zero.
    sql """INSERT INTO ivm_mm_join_l VALUES (0, NULL)"""
    sql """INSERT INTO ivm_mm_join_r VALUES (0), (1), (2), (3)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_mm_join_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ("replication_num" = "1", "ivm_minmax_top_k" = "1")
        AS SELECT MIN(l.v) AS min_v
        FROM ivm_mm_join_l l
        INNER JOIN ivm_mm_join_r r ON l.k = r.k
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
    sql """REFRESH MATERIALIZED VIEW ivm_mm_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_mm_join_mv")
    lastTaskId = sql_return_maparray("""
        SELECT TaskId
        FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}'
          AND MvName = 'ivm_mm_join_mv'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """)[0].TaskId.toString()
    order_qt_initial """
        SELECT min_v FROM ivm_mm_join_mv
    """

    // Insert L rows for R keys 1, 2 and 3, then delete R key 1. The join rewrite emits
    // +{1,1,2} from leftDelta JOIN rightPre and -{1} from leftPost JOIN rightDelta, so the
    // new minimum 1 has net count 1 (the delete cancels one of the two inserted rows).
    sql """INSERT INTO ivm_mm_join_l VALUES (1, 1), (2, 1), (3, 2)"""
    sql """DELETE FROM ivm_mm_join_r WHERE k = 1"""
    Thread.sleep(1000)
    sql """REFRESH MATERIALIZED VIEW ivm_mm_join_mv INCREMENTAL"""
    def canceledDeleteTask = latestIvmTask("ivm_mm_join_mv")
    assertEquals("SUCCESS", canceledDeleteTask.Status.toString())
    assertNoFallback(canceledDeleteTask)
    order_qt_after_canceled_delete """
        SELECT min_v FROM ivm_mm_join_mv
    """

    // The stored count for value 1 is 1. Deleting the remaining row that produces value 1
    // exhausts it, so the refresh must fall back instead of retaining the stale MIN = 1.
    sql """DELETE FROM ivm_mm_join_l WHERE k = 2"""
    Thread.sleep(1000)
    sql """REFRESH MATERIALIZED VIEW ivm_mm_join_mv INCREMENTAL FALLBACK"""
    def boundaryTask = latestIvmTask("ivm_mm_join_mv")
    assertEquals("SUCCESS", boundaryTask.Status.toString())
    assertEquals("COMPLETE", boundaryTask.RefreshMode.toString())
    assertEquals("MIN_MAX_BOUNDARY_HIT", boundaryTask.IvmFallbackReason.toString())
    order_qt_after_boundary """
        SELECT min_v FROM ivm_mm_join_mv
    """

    order_qt_source """
        SELECT MIN(l.v)
        FROM ivm_mm_join_l l
        INNER JOIN ivm_mm_join_r r ON l.k = r.k
    """
}
