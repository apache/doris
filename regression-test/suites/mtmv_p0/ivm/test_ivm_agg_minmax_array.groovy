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

// MIN/MAX over ARRAY values. The bounded top-K state only accepts scalar value types, so the
// default capacity resolves to 1 at CREATE and the state is the extreme plus its count. The
// incremental apply must therefore compare the values instead of calling LEAST/GREATEST, which
// the BE rejects for array arguments.
suite("test_ivm_agg_minmax_array") {
    // The base-table queries below assert the same rows as the MV, so transparent rewrite must be off.
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_dml_materialized_view_rewrite = false;"""
    sql """drop materialized view if exists ivm_array_mv"""
    sql """drop table if exists ivm_array_t"""

    sql """
        CREATE TABLE ivm_array_t (
            id BIGINT NOT NULL,
            category VARCHAR(16) NOT NULL,
            a ARRAY<INT>
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
    sql """INSERT INTO ivm_array_t VALUES
        (1, 'a', [1, 2]), (2, 'a', [3]), (3, 'a', [1, 2]),
        (4, 'b', [5]), (5, 'b', [9])"""

    // No ivm_minmax_top_k: the default capacity is above 1, but an ARRAY value cannot be held by
    // the top-K state, so CREATE resolves the capacity to 1.
    sql """
        CREATE MATERIALIZED VIEW ivm_array_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        KEY(category)
        DISTRIBUTED BY HASH(category) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT category, MIN(a) AS min_a, MAX(a) AS max_a, COUNT(*) AS row_count
        FROM ivm_array_t
        GROUP BY category
    """

    String createDdl = sql """SHOW CREATE MATERIALIZED VIEW ivm_array_mv"""
    assertTrue(createDdl.contains('"ivm_minmax_top_k" = "1"'),
            "an ARRAY argument must resolve the capacity to 1: " + createDdl)

    def assertNoFallback = { task ->
        def reason = task.IvmFallbackReason
        assertTrue(reason == null || reason.toString() == "\\N",
                "unexpected IVM fallback: ${reason}")
    }

    sql """REFRESH MATERIALIZED VIEW ivm_array_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_array_mv")
    order_qt_initial """SELECT category, min_a, max_a, row_count FROM ivm_array_mv"""

    // A smaller minimum enters category a and a larger maximum enters category b; both are
    // derivable from the extreme-plus-count state.
    sql """INSERT INTO ivm_array_t VALUES (6, 'a', [0, 9]), (7, 'b', [10])"""
    sql """REFRESH MATERIALIZED VIEW ivm_array_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_array_mv")
    def insertTask = latestIvmTask("ivm_array_mv")
    assertEquals("SUCCESS", insertTask.Status.toString())
    assertNoFallback(insertTask)
    order_qt_after_insert """SELECT category, min_a, max_a, row_count FROM ivm_array_mv"""

    // Deleting the only row that holds the minimum of category a moves the minimum back to
    // [1, 2], which an extreme-plus-count state cannot derive: the refresh falls back.
    sql """DELETE FROM ivm_array_t WHERE id = 6"""
    sql """REFRESH MATERIALIZED VIEW ivm_array_mv INCREMENTAL FALLBACK"""
    waitingMTMVTaskFinishedByMvName("ivm_array_mv")
    def deleteTask = latestIvmTask("ivm_array_mv")
    assertEquals("SUCCESS", deleteTask.Status.toString())
    assertEquals("COMPLETE", deleteTask.RefreshMode.toString())
    assertEquals("MIN_MAX_BOUNDARY_HIT", deleteTask.IvmFallbackReason.toString())
    order_qt_after_delete """SELECT category, min_a, max_a, row_count FROM ivm_array_mv"""

    // The source query must agree with the MV after every step.
    order_qt_source """SELECT category, MIN(a), MAX(a), COUNT(*) FROM ivm_array_t GROUP BY category"""
}
