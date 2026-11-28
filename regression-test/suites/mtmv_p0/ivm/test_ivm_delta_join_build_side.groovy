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

suite("test_ivm_delta_join_build_side") {
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"

    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_build_dup"
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_build_mow"
    // Refresh reparses the MV query in its own context. Use statement hints to control reorder there.
    def hintsByKind = [
        inner_ordered: "ORDERED",
        cross_ordered: "ORDERED",
        cross: "SET_VAR(disable_join_reorder=true)",
        cross_leading: "SET_VAR(disable_join_reorder=false) LEADING(l r)",
        cross_leading_disabled: "SET_VAR(disable_join_reorder=true) LEADING(l r)"
    ]
    def hintedViews = []
    for (def model : ["dup", "mow"]) {
        hintedViews.addAll(hintsByKind.keySet().collect { kind -> "ivm_build_${model}_${kind}" })
    }
    for (def mv : hintedViews) {
        sql "DROP MATERIALIZED VIEW IF EXISTS ${mv}"
    }
    sql "DROP TABLE IF EXISTS ivm_build_snapshot"
    sql "DROP TABLE IF EXISTS ivm_build_delta"

    sql """
        CREATE TABLE ivm_build_delta (k INT, value INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES ("replication_num" = "1", "changelog.enable" = "true")
    """
    sql """
        CREATE TABLE ivm_build_snapshot (k INT, value INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES ("replication_num" = "1", "changelog.enable" = "true")
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_build_dup
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ("replication_num" = "1")
        AS SELECT /*+ SET_VAR(disable_join_reorder=true) */ l.k, l.value AS lv, r.value AS rv
        FROM ivm_build_delta l INNER JOIN ivm_build_snapshot r ON l.k = r.k
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_build_mow
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ("replication_num" = "1", "ivm_force_mow" = "true")
        AS SELECT /*+ SET_VAR(disable_join_reorder=true) */ l.k, l.value AS lv, r.value AS rv
        FROM ivm_build_delta l INNER JOIN ivm_build_snapshot r ON l.k = r.k
    """
    for (def model : ["dup", "mow"]) {
        def modelProperty = model == "mow" ? ', "ivm_force_mow" = "true"' : ""
        for (def kind : hintsByKind.keySet()) {
            def join = kind.startsWith("inner") ? "INNER JOIN ivm_build_snapshot r ON l.k = r.k"
                                               : "CROSS JOIN ivm_build_snapshot r"
            sql """
                CREATE MATERIALIZED VIEW ivm_build_${model}_${kind}
                BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ("replication_num" = "1"${modelProperty})
                AS SELECT /*+ ${hintsByKind[kind]} */ l.k, l.value AS lv, r.value AS rv
                FROM ivm_build_delta l ${join}
            """
        }
    }

    sql "INSERT INTO ivm_build_delta VALUES (1, 10), (2, 20)"
    sql "INSERT INTO ivm_build_snapshot VALUES (1, 100), (1, 101)"
    for (def mv : ["ivm_build_dup", "ivm_build_mow"] + hintedViews) {
        sql "REFRESH MATERIALIZED VIEW ${mv} COMPLETE"
        waitingMTMVTaskFinishedByMvName(mv)
    }
    sql "INSERT INTO ivm_build_delta VALUES (1, 11), (NULL, 30)"
    // Make the snapshot size available independently of automatic statistics and BE reporting.
    sql """ALTER TABLE ivm_build_snapshot MODIFY COLUMN k SET STATS
           ('row_count'='2', 'ndv'='1', 'min_value'='1', 'max_value'='1')"""

    // Keep scan order, join type and the applied LEADING hint.
    def checkJoinAndScanOrder = { String tag, String mv ->
        def plan = sql_return_maparray "EXPLAIN SHAPE PLAN REFRESH MATERIALIZED VIEW ${mv} INCREMENTAL"
        def nodes = plan.findResults { row ->
            row["Explain String(Nereids Planner)"]
                    .find(/hashJoin\[[A-Z_]+|NestedLoopJoin\[[A-Z_]+\]|PhysicalOlapScan\[[^\]]+\]|^(?:Un)?Used: leading\([^)]+\)/)
                    ?.replace("hashJoin[", "")
        }
        try (def stmt = prepareStatement("SELECT ?")) {
            stmt.setString(1, nodes.join("\n"))
            delegate.quickRunTest(tag, stmt, false)
        }
    }

    checkJoinAndScanOrder("dup_cross_leading", "ivm_build_dup_cross_leading")
    checkJoinAndScanOrder("mow_cross_leading", "ivm_build_mow_cross_leading")
    checkJoinAndScanOrder("dup_inner", "ivm_build_dup")
    checkJoinAndScanOrder("mow_inner", "ivm_build_mow")
    for (def mv : hintedViews) {
        if (!mv.endsWith("_leading")) {
            checkJoinAndScanOrder(mv.substring("ivm_build_".length()), mv)
        }
    }
    sql "INSERT INTO ivm_build_snapshot VALUES (2, 200), (2, 201)"
    for (def mv : ["ivm_build_dup", "ivm_build_mow"]) {
        sql "REFRESH MATERIALIZED VIEW ${mv} INCREMENTAL"
        waitingMTMVTaskFinishedByMvName(mv)
    }
    order_qt_dup "SELECT k, lv, rv FROM ivm_build_dup"
    order_qt_mow "SELECT k, lv, rv FROM ivm_build_mow"
}
