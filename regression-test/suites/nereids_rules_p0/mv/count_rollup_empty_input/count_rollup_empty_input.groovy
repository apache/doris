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

suite("count_rollup_empty_input") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"
    sql "set enable_sql_cache = false"

    sql "DROP MATERIALIZED VIEW IF EXISTS rollup_empty_mv"
    sql "DROP TABLE IF EXISTS rollup_empty_base"
    sql "DROP TABLE IF EXISTS rollup_empty_agg"

    sql """
        CREATE TABLE rollup_empty_base (id INT, k INT)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO rollup_empty_base VALUES (1,1),(2,1),(3,2)"

    sql """
        CREATE MATERIALIZED VIEW rollup_empty_mv
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT k, COUNT(*) n FROM rollup_empty_base GROUP BY k
    """

    sql "REFRESH MATERIALIZED VIEW rollup_empty_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("rollup_empty_mv")

    sql "ANALYZE TABLE rollup_empty_base WITH SYNC"
    sql "ANALYZE TABLE rollup_empty_mv WITH SYNC"

    order_qt_mv_content "SELECT k, n FROM rollup_empty_mv"

    // The predicate filters out every row of the MV, but a count without GROUP BY still has to
    // output one row of 0, so the rolled up aggregate must not be a plain sum.
    mv_rewrite_success("SELECT /*+ use_mv(rollup_empty_mv) */ COUNT(*) FROM rollup_empty_base WHERE k > 999",
            "rollup_empty_mv")
    explain {
        sql("SELECT /*+ use_mv(rollup_empty_mv) */ COUNT(*) FROM rollup_empty_base WHERE k > 999")
        contains "sum0"
        notContains "non_nullable"
    }
    qt_rollup_empty_input "SELECT /*+ use_mv(rollup_empty_mv) */ COUNT(*) FROM rollup_empty_base WHERE k > 999"

    sql "SET enable_materialized_view_rewrite = false"
    qt_rollup_empty_input_without_mv "SELECT COUNT(*) FROM rollup_empty_base WHERE k > 999"
    sql "SET enable_materialized_view_rewrite = true"

    // Rows that do survive the filter keep the same counts as before.
    qt_rollup_partial_input "SELECT /*+ use_mv(rollup_empty_mv) */ COUNT(*) FROM rollup_empty_base WHERE k = 1"

    // A grouped count rollup never sees an empty group, so its results are unchanged either.
    order_qt_rollup_grouped "SELECT /*+ use_mv(rollup_empty_mv) */ k, COUNT(*) FROM rollup_empty_base GROUP BY k"

    order_qt_rollup_all "SELECT /*+ use_mv(rollup_empty_mv) */ COUNT(*) FROM rollup_empty_base"

    // A synchronous aggregate materialized view keeps COUNT as a SUM value column, so the rollup
    // reading it back has to keep pushing pre-aggregation down to storage instead of disabling it.
    sql """
        CREATE TABLE rollup_empty_agg (k INT, n BIGINT SUM DEFAULT "0")
        AGGREGATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO rollup_empty_agg VALUES (1, 2), (1, 1), (2, 1)"

    explain {
        sql("SELECT k, sum0(n) FROM rollup_empty_agg GROUP BY k")
        contains "PREAGGREGATION: ON"
    }
    order_qt_agg_preagg "SELECT k, sum0(n) FROM rollup_empty_agg GROUP BY k"
    order_qt_agg_preagg_sum "SELECT k, sum(n) FROM rollup_empty_agg GROUP BY k"
}
