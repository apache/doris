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

suite("test_cte_shared_producer_min_max_runtime_filter") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_pipeline_engine=true"
    // Materialize the CTE so that both references share one producer scan.
    sql "SET enable_cte_materialize=true"
    sql "SET inline_cte_referenced_threshold=0"
    sql "SET runtime_filter_type='MIN_MAX'"
    sql "SET enable_runtime_filter_prune=false"
    sql "SET runtime_filter_wait_time_ms=10000"

    sql "DROP TABLE IF EXISTS cte_rf_shared_producer_f"
    sql """
        CREATE TABLE cte_rf_shared_producer_f (
            k INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO cte_rf_shared_producer_f VALUES (0), (1), (5), (10)"

    sql "DROP TABLE IF EXISTS cte_rf_shared_producer_b"
    sql """
        CREATE TABLE cte_rf_shared_producer_b (
            x INT
        ) ENGINE=OLAP
        DUPLICATE KEY(x)
        DISTRIBUTED BY HASH(x) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO cte_rf_shared_producer_b VALUES (3)"

    sql "DROP TABLE IF EXISTS cte_rf_null_f"
    sql """
        CREATE TABLE cte_rf_null_f (
            k INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO cte_rf_null_f VALUES (NULL), (1), (3)"

    sql "DROP TABLE IF EXISTS cte_rf_null_b"
    sql """
        CREATE TABLE cte_rf_null_b (
            x INT
        ) ENGINE=OLAP
        DUPLICATE KEY(x)
        DISTRIBUTED BY HASH(x) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO cte_rf_null_b VALUES (NULL), (3)"

    // The two references of the CTE need disjoint row ranges: `c1.k > b.x` asks for a MIN runtime
    // filter and `c2.k < b.x` for a MAX one. Both target the same column of the shared producer, so
    // pushing both of them into the producer prunes every row (k >= 3 AND k <= 3) and the query
    // silently loses the rows each consumer still needs.
    sql "SET runtime_filter_mode='GLOBAL'"
    order_qt_shared_cte_min_max_rf_opposite_directions """
        WITH t AS (SELECT k, ABS(k) AS v FROM cte_rf_shared_producer_f)
        SELECT c2.k AS lo, c1.k AS hi, c2.v AS lo_v, c1.v AS hi_v, b.x
        FROM t c2 CROSS JOIN t c1 CROSS JOIN cte_rf_shared_producer_b b
        WHERE c1.k > b.x AND c2.k < b.x
        ORDER BY lo, hi
    """

    // Both references filter in the same direction, so both consumers produce the same runtime
    // filter. Pushing it into the producer is equivalent to applying it on every consumer.
    order_qt_shared_cte_min_max_rf_same_direction """
        WITH t AS (SELECT k, ABS(k) AS v FROM cte_rf_shared_producer_f)
        SELECT c2.k AS lo, c1.k AS hi, c2.v AS lo_v, c1.v AS hi_v, b.x
        FROM t c2 CROSS JOIN t c1 CROSS JOIN cte_rf_shared_producer_b b
        WHERE c1.k > b.x AND c2.k > b.x
        ORDER BY lo, hi
    """

    // Without a shared producer each reference gets its own scan and its own runtime filter.
    sql "SET enable_cte_materialize=false"
    order_qt_shared_cte_min_max_rf_inlined_cte """
        WITH t AS (SELECT k, ABS(k) AS v FROM cte_rf_shared_producer_f)
        SELECT c2.k AS lo, c1.k AS hi, c2.v AS lo_v, c1.v AS hi_v, b.x
        FROM t c2 CROSS JOIN t c1 CROSS JOIN cte_rf_shared_producer_b b
        WHERE c1.k > b.x AND c2.k < b.x
        ORDER BY lo, hi
    """
    sql "SET enable_cte_materialize=true"

    // NULL semantics are part of the filter identity: `c1.k <=> s.x` produces a null aware filter while the
    // deeper `c2.k = b0.x` produces an ordinary one. They prune different rows -- the ordinary filter removes
    // the rows whose probe column is NULL, which are exactly the rows the null aware predicate matches -- so
    // neither may be applied on the shared producer, otherwise the row which matches NULL with NULL is lost.
    sql "SET runtime_filter_type=12"
    order_qt_shared_cte_null_aware """
        WITH t AS (SELECT k FROM cte_rf_null_f)
        SELECT c1.k AS a, s.x AS bx
        FROM t c1
        JOIN (
            SELECT c2.k AS k2, b0.x AS x
            FROM t c2 RIGHT OUTER JOIN cte_rf_null_b b0 ON c2.k = b0.x
        ) s ON c1.k <=> s.x
        ORDER BY a, bx
    """

    // A value synthesizing node between the two builders makes the deeper filter prune rows the upper
    // consumer still needs: the repeat adds the NULL of the grouping set which does not group by x, so the
    // filter built below it, from `b.x = {3}`, must not be applied on the shared producer. The row which
    // matches that synthesized NULL with the NULL of the CTE is lost when it is.
    order_qt_shared_cte_grouping_sets """
        WITH t AS (SELECT k FROM cte_rf_null_f)
        SELECT c1.k AS a, g.x AS gx
        FROM t c1
        JOIN (
            SELECT x FROM (
                SELECT c2.k AS k2, b.x AS x
                FROM t c2 JOIN cte_rf_shared_producer_b b ON c2.k <=> b.x
            ) z GROUP BY GROUPING SETS ((x), ())
        ) g ON c1.k <=> g.x
        ORDER BY a, gx
    """

    sql "SET runtime_filter_mode='OFF'"
    order_qt_shared_cte_min_max_rf_off """
        WITH t AS (SELECT k, ABS(k) AS v FROM cte_rf_shared_producer_f)
        SELECT c2.k AS lo, c1.k AS hi, c2.v AS lo_v, c1.v AS hi_v, b.x
        FROM t c2 CROSS JOIN t c1 CROSS JOIN cte_rf_shared_producer_b b
        WHERE c1.k > b.x AND c2.k < b.x
        ORDER BY lo, hi
    """
}
