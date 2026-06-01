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

/**
 * DORIS-26101: FE local shuffle planner returns wrong result for
 * aggregate → CROSS/NLJ → downstream partitioned hash join.
 *
 * Root cause: PARTITIONED hash join used generic requireHash() which
 * resolved to LOCAL_EXECUTION_HASH_SHUFFLE. LOCAL hash has a different
 * modulus (per-BE instance count) than the GLOBAL hash exchange on
 * the other side of the join, causing instance mapping mismatch and
 * missing rows.
 *
 * Fix: PARTITIONED join uses requireGlobalExecutionHash() so any
 * inserted exchange matches the cross-fragment global exchange.
 */
suite("test_local_shuffle_cross_join_hash_join") {

    sql "DROP TABLE IF EXISTS ls_cross_a"
    sql "DROP TABLE IF EXISTS ls_cross_dim"

    sql """
        CREATE TABLE ls_cross_a (
            id INT,
            g INT,
            v INT
        ) ENGINE=OLAP DUPLICATE KEY(id, g)
        DISTRIBUTED BY HASH(id) BUCKETS 13
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE ls_cross_dim (
            id INT,
            g INT,
            w INT
        ) ENGINE=OLAP DUPLICATE KEY(id, g)
        DISTRIBUTED BY HASH(g) BUCKETS 17
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO ls_cross_a
        SELECT CAST(number AS INT) id, CAST(number AS INT) g, CAST(number * 10 + 1 AS INT) v
        FROM numbers("number" = "23")
    """

    sql """
        INSERT INTO ls_cross_dim
        SELECT CAST(number AS INT) id, CAST(number % 23 AS INT) g, CAST(1000 + number AS INT) w
        FROM numbers("number" = "713")
    """

    def commonHints = """/*+SET_VAR(
        enable_sql_cache=false,
        disable_join_reorder=true,
        enable_local_exchange_before_agg=false,
        experimental_force_to_local_shuffle=true,
        experimental_enable_parallel_scan=false,
        enable_runtime_filter_prune=false,
        enable_runtime_filter_partition_prune=false,
        runtime_filter_type='IN,MIN_MAX',
        parallel_pipeline_task_num=8,
        parallel_exchange_instance_num=8,
        query_timeout=600,
        prefer_join_method=shuffle
    )*/"""

    def queryBody = """
        SELECT x.g, COUNT(*) c, SUM(d.w) sw
        FROM (
            SELECT a.g, one.v
            FROM (
                SELECT g, SUM(v) sv
                FROM ls_cross_a
                GROUP BY g
            ) a
            CROSS JOIN (SELECT 1 v) one
        ) x
        JOIN [shuffle] ls_cross_dim d ON x.g = d.g
        GROUP BY x.g
        ORDER BY x.g
    """

    // Baseline: local shuffle OFF
    def baseline = sql """
        ${commonHints}
        SELECT ${commonHints.contains('x') ? '' : ''}
        /*+SET_VAR(enable_local_shuffle=false, enable_local_shuffle_planner=false)*/
        ${queryBody}
    """.toString().replace('SELECT /*+SET_VAR', 'SELECT /*+SET_VAR')

    // Actually run them properly:
    baseline = sql """
        SELECT /*+SET_VAR(
            enable_sql_cache=false,
            disable_join_reorder=true,
            enable_local_exchange_before_agg=false,
            experimental_force_to_local_shuffle=true,
            experimental_enable_parallel_scan=false,
            enable_runtime_filter_prune=false,
            enable_runtime_filter_partition_prune=false,
            runtime_filter_type='IN,MIN_MAX',
            parallel_pipeline_task_num=8,
            parallel_exchange_instance_num=8,
            query_timeout=600,
            prefer_join_method=shuffle,
            enable_local_shuffle=false,
            enable_local_shuffle_planner=false
        )*/ x.g, COUNT(*) c, SUM(d.w) sw
        FROM (
            SELECT a.g, one.v
            FROM (
                SELECT g, SUM(v) sv FROM ls_cross_a GROUP BY g
            ) a
            CROSS JOIN (SELECT 1 v) one
        ) x
        JOIN [shuffle] ls_cross_dim d ON x.g = d.g
        GROUP BY x.g
        ORDER BY x.g
    """

    // FE local shuffle planner
    def feResult = sql """
        SELECT /*+SET_VAR(
            enable_sql_cache=false,
            disable_join_reorder=true,
            enable_local_exchange_before_agg=false,
            experimental_force_to_local_shuffle=true,
            experimental_enable_parallel_scan=false,
            enable_runtime_filter_prune=false,
            enable_runtime_filter_partition_prune=false,
            runtime_filter_type='IN,MIN_MAX',
            parallel_pipeline_task_num=8,
            parallel_exchange_instance_num=8,
            query_timeout=600,
            prefer_join_method=shuffle,
            enable_local_shuffle=true,
            enable_local_shuffle_planner=true
        )*/ x.g, COUNT(*) c, SUM(d.w) sw
        FROM (
            SELECT a.g, one.v
            FROM (
                SELECT g, SUM(v) sv FROM ls_cross_a GROUP BY g
            ) a
            CROSS JOIN (SELECT 1 v) one
        ) x
        JOIN [shuffle] ls_cross_dim d ON x.g = d.g
        GROUP BY x.g
        ORDER BY x.g
    """

    assertEquals(23, baseline.size(), "baseline should return 23 rows")
    assertEquals(baseline, feResult,
        "DORIS-26101: FE local shuffle planner should match baseline for aggregate->CROSS JOIN->shuffle join")

    // Also test with NLJ (non-cross, inner join with always-true condition)
    def nljBaseline = sql """
        SELECT /*+SET_VAR(
            enable_sql_cache=false,
            disable_join_reorder=true,
            enable_local_exchange_before_agg=false,
            experimental_force_to_local_shuffle=true,
            experimental_enable_parallel_scan=false,
            enable_runtime_filter_prune=false,
            enable_runtime_filter_partition_prune=false,
            runtime_filter_type='IN,MIN_MAX',
            parallel_pipeline_task_num=8,
            parallel_exchange_instance_num=8,
            query_timeout=600,
            prefer_join_method=shuffle,
            enable_local_shuffle=false,
            enable_local_shuffle_planner=false
        )*/ x.g, COUNT(*) c, SUM(d.w) sw
        FROM (
            SELECT a.g
            FROM (
                SELECT g, SUM(v) sv FROM ls_cross_a GROUP BY g
            ) a, (SELECT 1 v) one
        ) x
        JOIN [shuffle] ls_cross_dim d ON x.g = d.g
        GROUP BY x.g
        ORDER BY x.g
    """

    def nljFeResult = sql """
        SELECT /*+SET_VAR(
            enable_sql_cache=false,
            disable_join_reorder=true,
            enable_local_exchange_before_agg=false,
            experimental_force_to_local_shuffle=true,
            experimental_enable_parallel_scan=false,
            enable_runtime_filter_prune=false,
            enable_runtime_filter_partition_prune=false,
            runtime_filter_type='IN,MIN_MAX',
            parallel_pipeline_task_num=8,
            parallel_exchange_instance_num=8,
            query_timeout=600,
            prefer_join_method=shuffle,
            enable_local_shuffle=true,
            enable_local_shuffle_planner=true
        )*/ x.g, COUNT(*) c, SUM(d.w) sw
        FROM (
            SELECT a.g
            FROM (
                SELECT g, SUM(v) sv FROM ls_cross_a GROUP BY g
            ) a, (SELECT 1 v) one
        ) x
        JOIN [shuffle] ls_cross_dim d ON x.g = d.g
        GROUP BY x.g
        ORDER BY x.g
    """

    assertEquals(nljBaseline, nljFeResult,
        "DORIS-26101: NLJ variant should also match baseline")
}
