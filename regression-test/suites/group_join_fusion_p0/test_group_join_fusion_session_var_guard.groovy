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

suite("test_group_join_fusion_session_var_guard") {
    sql "DROP VIEW IF EXISTS gj_guard_v"
    sql "DROP TABLE IF EXISTS gj_guard_l"
    sql "DROP TABLE IF EXISTS gj_guard_r"

    sql """
        CREATE TABLE gj_guard_l (k INT NOT NULL, v INT NULL) ENGINE=OLAP
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """
    sql """
        CREATE TABLE gj_guard_r (k INT NOT NULL, v INT NULL) ENGINE=OLAP
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """
    sql "INSERT INTO gj_guard_l VALUES (1, 10), (1, 20), (2, 5)"
    sql "INSERT INTO gj_guard_r VALUES (1, 7), (2, 3)"

    // Same shape as the view defined below, over base tables.
    def inlineQuery = """
        SELECT l.k, SUM(l.v) FROM gj_guard_l l JOIN [shuffle] gj_guard_r r
        ON l.k = r.k GROUP BY l.k ORDER BY l.k
        """

    sql "SET agg_phase = 1"
    sql "SET eager_aggregation_mode = -1"
    sql "SET eager_agg_broadcast_row_count = 0"
    sql "SET enable_bucketed_hash_agg = false"
    sql "SET enable_bucket_shuffle_join = false"
    sql "SET disable_join_reorder = true"
    sql "SET experimental_use_serial_exchange = false"
    sql "SET runtime_filter_mode = 'OFF'"
    sql "SET parallel_pipeline_task_num = 1"
    sql "SET enable_spill = false"
    sql "SET enable_group_join_fusion = true"

    // Positive control: the same query over base tables fuses, so the guard below - not the
    // shape or the session settings - is what keeps the view query unfused.
    explain {
        sql inlineQuery
        contains("VGROUP JOIN")
    }
    order_qt_inline inlineQuery

    // A view records the session variables it was created under. enable_decimal256 affects the
    // query result, so querying the view under a different value wraps its expressions in
    // SessionVarGuardExpr, whose type must be computed with the creation-time variables. The
    // fused operator translates the bare AggregateExpression and would drop that guard, so such
    // an aggregate must stay on the regular HashJoin + Aggregate path.
    sql "SET enable_decimal256 = true"
    sql """
        CREATE VIEW gj_guard_v AS
        SELECT l.k AS k, SUM(l.v) AS s FROM gj_guard_l l JOIN [shuffle] gj_guard_r r
        ON l.k = r.k GROUP BY l.k
        """
    sql "SET enable_decimal256 = false"

    explain {
        sql "SELECT k, s FROM gj_guard_v ORDER BY k"
        notContains("VGROUP JOIN")
    }
    order_qt_guarded_view "SELECT k, s FROM gj_guard_v ORDER BY k"

    // Restore defaults so other suites are not affected.
    sql "SET enable_group_join_fusion = false"
    sql "SET enable_decimal256 = false"
    sql "SET agg_phase = 0"
    sql "SET disable_join_reorder = false"
    sql "SET runtime_filter_mode = 'GLOBAL'"
    sql "SET parallel_pipeline_task_num = 0"
}
