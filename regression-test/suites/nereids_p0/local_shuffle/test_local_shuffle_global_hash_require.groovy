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
 * DORIS-26100 / DORIS-26101: FE local shuffle planner inserts
 * LOCAL_EXECUTION_HASH_SHUFFLE before downstream PARTITIONED consumers
 * (hash join, intersect/except). LOCAL hash has per-BE modulus, incompatible
 * with the global exchange on the sibling side.
 *
 * Fix: PARTITIONED hash join and Intersect/Except use
 * requireGlobalExecutionHash() so any inserted exchange uses GLOBAL modulus.
 *
 * Each case compares FE planner result against local-shuffle-off baseline.
 */
suite("test_local_shuffle_global_hash_require") {

    def feHints = """/*+SET_VAR(
        enable_sql_cache=false, disable_join_reorder=true,
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
    )*/"""

    def offHints = """/*+SET_VAR(
        enable_sql_cache=false, disable_join_reorder=true,
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
    )*/"""

    // ============================================================
    // DORIS-26101: aggregate -> CROSS JOIN -> shuffle hash join
    // ============================================================
    sql "DROP TABLE IF EXISTS ls_cross_a"
    sql "DROP TABLE IF EXISTS ls_cross_dim"
    sql """CREATE TABLE ls_cross_a (id INT, g INT, v INT)
           ENGINE=OLAP DUPLICATE KEY(id,g) DISTRIBUTED BY HASH(id) BUCKETS 13
           PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE ls_cross_dim (id INT, g INT, w INT)
           ENGINE=OLAP DUPLICATE KEY(id,g) DISTRIBUTED BY HASH(g) BUCKETS 17
           PROPERTIES ("replication_num"="1")"""
    sql """INSERT INTO ls_cross_a
           SELECT CAST(number AS INT), CAST(number AS INT), CAST(number*10+1 AS INT)
           FROM numbers("number"="23")"""
    sql """INSERT INTO ls_cross_dim
           SELECT CAST(number AS INT), CAST(number%23 AS INT), CAST(1000+number AS INT)
           FROM numbers("number"="713")"""

    def cross_baseline = sql """SELECT ${offHints} x.g, COUNT(*) c, SUM(d.w) sw
        FROM (SELECT a.g, one.v FROM (SELECT g, SUM(v) sv FROM ls_cross_a GROUP BY g) a
              CROSS JOIN (SELECT 1 v) one) x
        JOIN [shuffle] ls_cross_dim d ON x.g = d.g
        GROUP BY x.g ORDER BY x.g"""

    def cross_fe = sql """SELECT ${feHints} x.g, COUNT(*) c, SUM(d.w) sw
        FROM (SELECT a.g, one.v FROM (SELECT g, SUM(v) sv FROM ls_cross_a GROUP BY g) a
              CROSS JOIN (SELECT 1 v) one) x
        JOIN [shuffle] ls_cross_dim d ON x.g = d.g
        GROUP BY x.g ORDER BY x.g"""

    assertEquals(23, cross_baseline.size())
    assertEquals(cross_baseline, cross_fe,
        "DORIS-26101: aggregate -> CROSS JOIN -> shuffle join")

    // ============================================================
    // DORIS-26100 case 1: aggregate -> table function -> shuffle join
    // ============================================================
    sql "DROP TABLE IF EXISTS ls_tf_a"
    sql "DROP TABLE IF EXISTS ls_tf_dim"
    sql """CREATE TABLE ls_tf_a (id INT, g INT, s VARCHAR(64))
           ENGINE=OLAP DUPLICATE KEY(id,g) DISTRIBUTED BY HASH(id) BUCKETS 11
           PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE ls_tf_dim (g INT, w INT)
           ENGINE=OLAP DUPLICATE KEY(g) DISTRIBUTED BY HASH(g) BUCKETS 13
           PROPERTIES ("replication_num"="1")"""
    sql """INSERT INTO ls_tf_a VALUES
           (0,0,'a,b'),(1,1,'c'),(2,2,''),(3,3,null),(4,4,'d,e,f'),(5,5,'z')"""
    sql """INSERT INTO ls_tf_dim VALUES
           (0,1),(1,11),(2,21),(3,31),(4,41),(5,51),(6,61),(7,71)"""

    def tf_baseline = sql """SELECT ${offHints} x.g, COUNT(*) c, SUM(d.w) sw
        FROM (SELECT a.g, e FROM (SELECT g, MAX(s) s FROM ls_tf_a GROUP BY g) a
              LATERAL VIEW explode_split_outer(a.s, ',') lv AS e) x
        JOIN [shuffle] ls_tf_dim d ON x.g=d.g
        GROUP BY x.g ORDER BY x.g"""

    def tf_fe = sql """SELECT ${feHints} x.g, COUNT(*) c, SUM(d.w) sw
        FROM (SELECT a.g, e FROM (SELECT g, MAX(s) s FROM ls_tf_a GROUP BY g) a
              LATERAL VIEW explode_split_outer(a.s, ',') lv AS e) x
        JOIN [shuffle] ls_tf_dim d ON x.g=d.g
        GROUP BY x.g ORDER BY x.g"""

    assertEquals(6, tf_baseline.size())
    assertEquals(tf_baseline, tf_fe,
        "DORIS-26100: aggregate -> table function -> shuffle join")

    // ============================================================
    // DORIS-26100 case 2: aggregate -> NAAJ -> shuffle join
    // ============================================================
    def naajHints = { ls_on ->
        """/*+SET_VAR(
            enable_sql_cache=false, disable_join_reorder=true,
            disable_colocate_plan=true,
            auto_broadcast_join_threshold=-1, broadcast_row_count_limit=0,
            experimental_force_to_local_shuffle=true,
            experimental_enable_parallel_scan=false,
            enable_runtime_filter_prune=false,
            enable_runtime_filter_partition_prune=false,
            runtime_filter_type='IN,MIN_MAX',
            parallel_pipeline_task_num=16,
            parallel_exchange_instance_num=8,
            query_timeout=600,
            enable_local_shuffle=${ls_on},
            enable_local_shuffle_planner=${ls_on}
        )*/"""
    }

    sql "DROP TABLE IF EXISTS ls_naaj_a"
    sql "DROP TABLE IF EXISTS ls_naaj_bnn"
    sql "DROP TABLE IF EXISTS ls_naaj_dim"
    sql """CREATE TABLE ls_naaj_a (k INT, g INT, v INT)
           ENGINE=OLAP DUPLICATE KEY(k,g) DISTRIBUTED BY HASH(k) BUCKETS 17
           PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE ls_naaj_bnn (g INT)
           ENGINE=OLAP DUPLICATE KEY(g) DISTRIBUTED BY HASH(g) BUCKETS 13
           PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE ls_naaj_dim (g INT, w INT)
           ENGINE=OLAP DUPLICATE KEY(g) DISTRIBUTED BY HASH(g) BUCKETS 17
           PROPERTIES ("replication_num"="1")"""
    sql """INSERT INTO ls_naaj_a
           SELECT CAST(number AS INT), CAST(number%17 AS INT), CAST(number*10+1 AS INT)
           FROM numbers("number"="68")"""
    sql """INSERT INTO ls_naaj_bnn
           SELECT CAST(number AS INT) FROM numbers("number"="17")
           WHERE number NOT IN (1,7,8,9,14,16)"""
    sql """INSERT INTO ls_naaj_dim
           SELECT CAST(number%17 AS INT), CAST(100+number AS INT)
           FROM numbers("number"="85")"""

    def naaj_baseline = sql """SELECT ${naajHints('false')} y.g, COUNT(*) AS c, SUM(d.w) AS s
        FROM (SELECT x.g FROM (SELECT g, COUNT(*) cnt FROM ls_naaj_a GROUP BY g) x
              WHERE x.g NOT IN (SELECT g FROM ls_naaj_bnn)) y
        JOIN [shuffle] ls_naaj_dim d ON y.g = d.g
        GROUP BY y.g ORDER BY y.g"""

    def naaj_fe = sql """SELECT ${naajHints('true')} y.g, COUNT(*) AS c, SUM(d.w) AS s
        FROM (SELECT x.g FROM (SELECT g, COUNT(*) cnt FROM ls_naaj_a GROUP BY g) x
              WHERE x.g NOT IN (SELECT g FROM ls_naaj_bnn)) y
        JOIN [shuffle] ls_naaj_dim d ON y.g = d.g
        GROUP BY y.g ORDER BY y.g"""

    assertEquals(6, naaj_baseline.size())
    assertEquals(naaj_baseline, naaj_fe,
        "DORIS-26100: aggregate -> NAAJ -> shuffle join")

    // ============================================================
    // DORIS-26100 case 3: analytic (ROW_NUMBER) -> shuffle join
    // ============================================================
    def analyticHints = { ls_on ->
        """/*+SET_VAR(
            enable_sql_cache=false, disable_join_reorder=true,
            disable_colocate_plan=true,
            auto_broadcast_join_threshold=-1, broadcast_row_count_limit=0,
            experimental_force_to_local_shuffle=true,
            experimental_enable_parallel_scan=false,
            enable_runtime_filter_prune=false,
            enable_runtime_filter_partition_prune=false,
            runtime_filter_type='IN,MIN_MAX',
            parallel_pipeline_task_num=16,
            parallel_exchange_instance_num=8,
            query_timeout=600,
            ignore_storage_data_distribution=false,
            use_serial_exchange=false,
            experimental_use_serial_exchange=false,
            enable_local_shuffle=${ls_on},
            enable_local_shuffle_planner=${ls_on}
        )*/"""
    }

    sql "DROP TABLE IF EXISTS ls_analytic_a"
    sql "DROP TABLE IF EXISTS ls_analytic_dim"
    sql """CREATE TABLE ls_analytic_a (pk INT, g INT, v INT)
           ENGINE=OLAP DUPLICATE KEY(pk,g) DISTRIBUTED BY HASH(pk) BUCKETS 13
           PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE ls_analytic_dim (g INT, w INT)
           ENGINE=OLAP DUPLICATE KEY(g) DISTRIBUTED BY HASH(g) BUCKETS 17
           PROPERTIES ("replication_num"="1")"""
    sql """INSERT INTO ls_analytic_a
           SELECT CAST(number AS INT), CAST(number%23 AS INT), CAST(number*10+1 AS INT)
           FROM numbers("number"="920")"""
    sql """INSERT INTO ls_analytic_dim
           SELECT CAST(number%23 AS INT), CAST(1000+number AS INT)
           FROM numbers("number"="713")"""

    def analytic_baseline = sql """SELECT ${analyticHints('false')}
        x.g, COUNT(*) AS c, SUM(x.rn) AS srn, SUM(d.w) AS sw
        FROM (SELECT g, pk, ROW_NUMBER() OVER(PARTITION BY g ORDER BY pk) AS rn
              FROM ls_analytic_a) x
        JOIN [shuffle] ls_analytic_dim d ON x.g = d.g
        GROUP BY x.g ORDER BY x.g"""

    def analytic_fe = sql """SELECT ${analyticHints('true')}
        x.g, COUNT(*) AS c, SUM(x.rn) AS srn, SUM(d.w) AS sw
        FROM (SELECT g, pk, ROW_NUMBER() OVER(PARTITION BY g ORDER BY pk) AS rn
              FROM ls_analytic_a) x
        JOIN [shuffle] ls_analytic_dim d ON x.g = d.g
        GROUP BY x.g ORDER BY x.g"""

    assertEquals(23, analytic_baseline.size())
    assertEquals(analytic_baseline, analytic_fe,
        "DORIS-26100: analytic -> shuffle join")

    // ============================================================
    // DORIS-26100 case 4: analytic -> INTERSECT
    // ============================================================
    sql "DROP TABLE IF EXISTS ls_aniset_a"
    sql "DROP TABLE IF EXISTS ls_aniset_dim"
    sql """CREATE TABLE ls_aniset_a (pk INT, g INT, v INT)
           ENGINE=OLAP DUPLICATE KEY(pk,g) DISTRIBUTED BY HASH(pk) BUCKETS 13
           PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE ls_aniset_dim (g INT, w INT)
           ENGINE=OLAP DUPLICATE KEY(g) DISTRIBUTED BY HASH(g) BUCKETS 17
           PROPERTIES ("replication_num"="1")"""
    sql """INSERT INTO ls_aniset_a
           SELECT CAST(number AS INT), CAST(number%23 AS INT), CAST(number*10+1 AS INT)
           FROM numbers("number"="920")"""
    sql """INSERT INTO ls_aniset_dim
           SELECT CAST(number AS INT), CAST(100+number AS INT)
           FROM numbers("number"="23")"""

    def intersectHints = { ls_on ->
        """/*+SET_VAR(
            enable_sql_cache=false, disable_join_reorder=true,
            disable_colocate_plan=true,
            auto_broadcast_join_threshold=-1, broadcast_row_count_limit=0,
            experimental_force_to_local_shuffle=true,
            experimental_enable_parallel_scan=false,
            enable_runtime_filter_prune=false,
            enable_runtime_filter_partition_prune=false,
            runtime_filter_type='IN,MIN_MAX',
            parallel_pipeline_task_num=16,
            parallel_exchange_instance_num=8,
            query_timeout=600,
            ignore_storage_data_distribution=false,
            use_serial_exchange=false,
            experimental_use_serial_exchange=false,
            enable_partition_topn=false,
            enable_local_shuffle=${ls_on},
            enable_local_shuffle_planner=${ls_on}
        )*/"""
    }

    def intersect_baseline = sql """SELECT ${intersectHints('false')} g FROM (
        SELECT g FROM (SELECT g, ROW_NUMBER() OVER(PARTITION BY g ORDER BY pk) AS rn
                       FROM ls_aniset_a) x WHERE rn > 0
        INTERSECT
        SELECT g FROM ls_aniset_dim) t ORDER BY g"""

    def intersect_fe = sql """SELECT ${intersectHints('true')} g FROM (
        SELECT g FROM (SELECT g, ROW_NUMBER() OVER(PARTITION BY g ORDER BY pk) AS rn
                       FROM ls_aniset_a) x WHERE rn > 0
        INTERSECT
        SELECT g FROM ls_aniset_dim) t ORDER BY g"""

    assertEquals(23, intersect_baseline.size())
    assertEquals(intersect_baseline, intersect_fe,
        "DORIS-26100: analytic -> INTERSECT")
}
