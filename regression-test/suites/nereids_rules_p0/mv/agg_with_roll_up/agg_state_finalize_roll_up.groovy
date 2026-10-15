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

suite("agg_state_finalize_roll_up") {
    String db = context.config.getDbNameByFile(context.file)
    sql "set enable_agg_state=true"
    sql "set pre_materialized_view_rewrite_strategy=TRY_IN_RBO"
    for (String mv : ["finalize_max_mv", "finalize_abs_mv", "finalize_sum_mv", "finalize_union_mv"]) {
        sql "drop materialized view if exists ${mv}"
    }
    sql "drop table if exists agg_finalize_rollup_input"
    sql """
        create table agg_finalize_rollup_input (k int, g int, v double)
        duplicate key(k, g) distributed by hash(k) buckets 1
        properties("replication_num"="1")
    """
    sql """
        insert into agg_finalize_rollup_input values
            (1, 1, -4), (1, 1, 3), (1, 2, -2), (1, 2, 7),
            (2, 1, 5), (2, 2, null), (3, 1, null)
    """
    // Make the finer-group MV cheaper than scanning the base table so the test executes its rollup.
    sql """
        insert into agg_finalize_rollup_input
        select k, g, v from agg_finalize_rollup_input cross join numbers("number"="99")
    """
    sql "analyze table agg_finalize_rollup_input with sync"

    String combinedQuery = """
        select k, sum_finalize(sum_combine(avg_finalize(avg_state(v))))
        from agg_finalize_rollup_input group by k
    """
    String plainQuery = """
        select k, sum(avg_finalize(avg_state(v)))
        from agg_finalize_rollup_input group by k
    """
    order_qt_combined_before combinedQuery
    order_qt_plain_before plainQuery

    // Sharing the inner AVG finalizer must not make SUM compatible with MAX.
    create_async_mv(db, "finalize_max_mv", """
        select k, g, max_combine(avg_finalize(avg_state(v))) s
        from agg_finalize_rollup_input group by k, g
    """)
    mv_rewrite_fail(combinedQuery, "finalize_max_mv")
    mv_rewrite_fail(plainQuery, "finalize_max_mv")
    order_qt_different_aggregate combinedQuery

    // The full value expression matters even when both outer aggregates are SUM.
    create_async_mv(db, "finalize_abs_mv", """
        select k, g, sum_combine(abs(avg_finalize(avg_state(v)))) s
        from agg_finalize_rollup_input group by k, g
    """)
    mv_rewrite_fail(combinedQuery, "finalize_abs_mv")
    mv_rewrite_fail(plainQuery, "finalize_abs_mv")
    order_qt_different_argument combinedQuery

    create_async_mv(db, "finalize_sum_mv", """
        select k, g, sum_combine(avg_finalize(avg_state(v))) s
        from agg_finalize_rollup_input group by k, g
    """)
    mv_rewrite_success(combinedQuery, "finalize_sum_mv")
    mv_rewrite_success(plainQuery, "finalize_sum_mv")
    order_qt_combined_after combinedQuery
    order_qt_plain_after plainQuery

    // Existing MERGE/UNION/STATE rollup remains available.
    create_async_mv(db, "finalize_union_mv", """
        select k, g, sum_union(sum_state(v)) s
        from agg_finalize_rollup_input group by k, g
    """)
    String mergeQuery = """
        select k, sum_merge(sum_state(v))
        from agg_finalize_rollup_input group by k
    """
    mv_rewrite_success(mergeQuery, "finalize_union_mv")
    order_qt_merge_after mergeQuery
}
