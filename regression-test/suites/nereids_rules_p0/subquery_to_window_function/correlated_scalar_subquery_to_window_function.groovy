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

suite("correlated_scalar_subquery_to_window_function") {
    multi_sql """
        set exec_mem_limit=21G;
        set be_number_for_test=3;
        set enable_runtime_filter_prune=false;
        set parallel_pipeline_task_num=8;
        set forbid_unknown_col_stats=false;
        set enable_stats=true;
        set runtime_filter_type=8;
        set broadcast_row_count_limit = 0;
        set enable_nereids_timeout = false;
        set enable_pipeline_engine = true;
        set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        set push_topn_to_agg = true;
        set topn_opt_limit_threshold=1024;
    """

    sql "DROP TABLE IF EXISTS fact FORCE"
    sql "DROP TABLE IF EXISTS dim FORCE"
    sql "DROP TABLE IF EXISTS dim_unique FORCE"

    sql """
        CREATE TABLE fact (
          id INT,
          k INT,
          v INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """

    sql """
        CREATE TABLE dim (
          did INT,
          k INT,
          tag INT
        ) ENGINE=OLAP
        DUPLICATE KEY(did)
        DISTRIBUTED BY HASH(did) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """

    // dim_unique: UNIQUE KEY on k so the correlated outer-only slot is unique
    // and non-null.  This table exercises the AggScalarSubQueryToWindowFunction
    // rewrite path (WinMagic).
    sql """
        CREATE TABLE dim_unique (
          k INT NOT NULL,
          did INT,
          tag INT
        ) ENGINE=OLAP
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """

    sql """ALTER TABLE dim_unique ADD CONSTRAINT uq_dim_unique_k UNIQUE (k)"""

    sql """
        insert into fact values
            (1, 1, 5),
            (2, 1, 7),
            (3, 2, 4),
            (4, 2, 10),
            (5, 3, 8),
            (10, 1, 6),
            (11, 2, 6)
    """

    sql """
        insert into dim values
            (10, 1, 1),
            (11, 1, 1),
            (20, 2, 1),
            (30, 3, 0),
            (31, null, 1)
    """

    sql """
        insert into dim_unique values
            (1, 10, 1),
            (2, 20, 1),
            (3, 30, 0),
            (4, 40, 3)
    """

    // Positive case: dim_unique has UNIQUE KEY(k), so the correlated slot is
    // unique + non-null and the rule rewrites the scalar subquery to a window
    // function.
    order_qt_d26072_unique """
        SELECT d.did, f.id, f.k, f.v
        FROM fact f, dim_unique d
        WHERE f.k = d.k
          AND f.v * 2 > (
            SELECT SUM(f2.v)
            FROM fact f2
            WHERE f2.k = d.k
          )
        ORDER BY d.did, f.id
    """

    // Negative case (no rewrite): dim is DUPLICATE KEY(did) – k is neither
    // unique nor guaranteed non-null, so the rule must not rewrite this to a
    // window function.  The query validates the original scalar-subquery plan.
    order_qt_d26072 """
        SELECT d.did, f.id, f.k, f.v
        FROM fact f, dim d
        WHERE f.k = d.k
          AND f.v * 2 > (
            SELECT SUM(f2.v)
            FROM fact f2
            WHERE f2.k = d.k
          )
        ORDER BY d.did, f.id
    """

    // Negative case (no rewrite): the outer-only relation output is pruned
    // (dim columns are projected away in the subquery), so the rule cannot
    // establish the outer-only table and must not rewrite.
    order_qt_d26072_no_change """
        SELECT t.id, t.k, t.v
        FROM (
            SELECT f.id, d.k, f.v
            FROM fact f, dim d
            WHERE f.k = d.k
        ) t
        WHERE t.v * 2 > (
            SELECT SUM(f2.v)
            FROM fact f2
            WHERE f2.k = t.k
          )
        ORDER BY t.id, t.k, t.v
    """

    // Shape plan: positive case — dim_unique has UNIQUE KEY(k), the rewrite
    // should produce a LogicalWindow node.
    qt_d26072_unique_shape """
        explain shape plan
        SELECT d.did, f.id, f.k, f.v
        FROM fact f, dim_unique d
        WHERE f.k = d.k
          AND f.v * 2 > (
            SELECT SUM(f2.v)
            FROM fact f2
            WHERE f2.k = d.k
          )
        ORDER BY d.did, f.id
    """

    // Shape plan: negative case — dim is DUPLICATE KEY(did), no rewrite,
    // LogicalWindow must NOT appear.
    qt_d26072_shape """
        explain shape plan
        SELECT d.did, f.id, f.k, f.v
        FROM fact f, dim d
        WHERE f.k = d.k
          AND f.v * 2 > (
            SELECT SUM(f2.v)
            FROM fact f2
            WHERE f2.k = d.k
          )
        ORDER BY d.did, f.id
    """

    // ------ Shared-table predicate must stay above the window ------
    // f.v > 5 references only the shared table (fact).  It must stay ABOVE
    // the window so the window function aggregates ALL fact rows per key,
    // not just the filtered subset.
    //
    // fact rows per key: k=1 → v=5,7,6 (sum=18); k=2 → v=4,10,6 (sum=20)
    // With f.v > 5 above window: k=1 sum=18, none of (7,6) pass 2*v>18;
    // k=2 sum=20, only v=10 passes 20>20→false.  Results: none.
    // Bug behavior (f.v > 5 below window): k=1 sum over v>5=7+6=13,
    // v=7→14>13 true; k=2 sum over v>5=10+6=16, v=10→20>16 true.
    // Both rows incorrectly returned.
    order_qt_shared_filter_above_window """
        SELECT d.did, f.id, f.k, f.v
        FROM fact f, dim_unique d
        WHERE f.k = d.k
          AND f.v > 5
          AND f.v * 2 > (
            SELECT SUM(f2.v)
            FROM fact f2
            WHERE f2.k = d.k
          )
        ORDER BY d.did, f.id
    """

    // Shape: shared-filter rewrite — LogicalWindow must be present.
    qt_shared_filter_above_window_shape """
        explain shape plan
        SELECT d.did, f.id, f.k, f.v
        FROM fact f, dim_unique d
        WHERE f.k = d.k
          AND f.v > 5
          AND f.v * 2 > (
            SELECT SUM(f2.v)
            FROM fact f2
            WHERE f2.k = d.k
          )
        ORDER BY d.did, f.id
    """

    // ------ Mixed predicate f.v > d.tag must stay above the window ------
    // f.v > d.tag references both shared (f.v) and outer-only (d.tag) columns.
    // It must stay ABOVE so the window computes over all joined rows.
    //
    // fact: k=1→v=5,7,6; k=2→v=4,10,6
    // dim_unique: k=1 tag=1; k=2 tag=1; k=3 tag=0; k=4 tag=3
    //
    // k=1: all fact rows: 5>1✓, 7>1✓, 6>1✓; sum=18;  2*v > 18: 14>18✗, 12>18✗
    // k=2: all fact rows: 4>1✓, 10>1✓, 6>1✓; sum=20; 2*v > 20: 20>20✗, 12>20✗
    // k=3: only fact v=8, 8>0✓; sum=8; 2*8>8✓ → (30, 5, 3, 8)
    // k=4: 5>3✓, 7>3✓, 6>3✓; sum=18; 2*v>18: none pass
    // Also: fact row with k=1 joins dim_unique k=3? No — join is f.k=d.k.
    // Expected: k=3 row only: (30, 5, 3, 8)
    order_qt_mixed_above_window """
        SELECT d.did, f.id, f.k, f.v
        FROM fact f, dim_unique d
        WHERE f.k = d.k
          AND f.v > d.tag
          AND f.v * 2 > (
            SELECT SUM(f2.v)
            FROM fact f2
            WHERE f2.k = d.k
          )
        ORDER BY d.did, f.id
    """

    // Shape: mixed predicate rewrite — LogicalWindow must be present.
    qt_mixed_above_window_shape """
        explain shape plan
        SELECT d.did, f.id, f.k, f.v
        FROM fact f, dim_unique d
        WHERE f.k = d.k
          AND f.v > d.tag
          AND f.v * 2 > (
            SELECT SUM(f2.v)
            FROM fact f2
            WHERE f2.k = d.k
          )
        ORDER BY d.did, f.id
    """

    // ------ Inner filter conjunct stays below the window ------
    // The inner subquery has f2.v < 10.  checkFilter() matches this against
    // the outer conjunct f.v < 10.  The matched conjunct must stay BELOW
    // the window because it's semantically part of the inner aggregate's filter.
    //
    // fact: k=1→v=5,7,6; k=2→v=4,10,6; k=3→v=8
    // Inner agg over f2.v < 10: k=1→5,7,6 sum=18; k=2→4,6 sum=10 (v=10 excluded);
    //   k=3→8 sum=8
    // Outer: f.v < 10 excludes v=10.  f.v*2 > sum: k=1→14>18✗,12>18✗,10>18✗;
    //   k=2→8>10✗,12>10✓ → (20, 11, 2, 6); k=3→16>8✓ → (30, 5, 3, 8)
    // Expected: (20, 11, 2, 6), (30, 5, 3, 8)
    //
    // Note: When the inner filter f2.v < 10 is kept as a separate LogicalFilter
    // (not pushed into the scan), the rewrite SHOULD match per the UT tests.
    // In the full optimizer pipeline, PullUpCorrelatedFilterUnderApplyAggregateProject
    // may restructure the plan so the pattern no longer matches.  The result
    // data is verified regardless; the shape documents the actual plan.
    order_qt_inner_filter_below_window """
        SELECT d.did, f.id, f.k, f.v
        FROM fact f, dim_unique d
        WHERE f.k = d.k
          AND f.v < 10
          AND f.v * 2 > (
            SELECT SUM(f2.v)
            FROM fact f2
            WHERE f2.k = d.k
              AND f2.v < 10
          )
        ORDER BY d.did, f.id
    """

    // Shape: inner filter conjunct rewrite — LogicalWindow must be present
    // when the rule matches.  Documents the actual plan produced.
    qt_inner_filter_below_window_shape """
        explain shape plan
        SELECT d.did, f.id, f.k, f.v
        FROM fact f, dim_unique d
        WHERE f.k = d.k
          AND f.v < 10
          AND f.v * 2 > (
            SELECT SUM(f2.v)
            FROM fact f2
            WHERE f2.k = d.k
              AND f2.v < 10
          )
        ORDER BY d.did, f.id
    """
}