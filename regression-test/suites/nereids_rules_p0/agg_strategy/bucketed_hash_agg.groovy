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

suite("bucketed_hash_agg") {
    // ============================================================
    // Test: Bucketed Hash Aggregation regression
    //
    // Verifies that on single-BE deployments with enable_bucketed_hash_agg=true,
    // the translator fuses one-phase GLOBAL hash aggregate + distribute into
    // a single BUCKETED AGGREGATE operator, eliminating exchange overhead.
    // On multi-BE deployments, bucketed agg must NOT be used.
    // ============================================================

    // --- session settings ---
    sql "set enable_nereids_planner=true"
    sql "set enable_parallel_result_sink=false"
    sql "set runtime_filter_mode=OFF"
    sql "set parallel_pipeline_task_num=2"
    sql "set bucketed_agg_min_input_rows=0"
    // Bucketed agg is disabled while spill is enabled, so turn off fuzzy spill.
    sql "set enable_spill=false"
    sql "set enable_force_spill=false"
    sql "set bucketed_agg_max_group_keys=0"
    // The table below is never analyzed, so group-by column stats are unknown and
    // StatsCalculator falls back to rows * DEFAULT_AGGREGATE_RATIO (1/3.0) for the
    // aggregate output cardinality. With the default bucketed_agg_high_card_threshold
    // (0.3), bucketedDataVolumeGatesPass rejects the pattern (rows/3 > rows*0.3),
    // so raise the threshold to make the positive fusion test deterministic.
    sql "set bucketed_agg_high_card_threshold=1.0"

    // --- create test table ---
    sql """ DROP TABLE IF EXISTS bucketed_agg_reg_test; """
    sql """
        CREATE TABLE bucketed_agg_reg_test (
            id int,
            grp varchar(20),
            val bigint
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES('replication_num' = '1');
    """
    sql """ INSERT INTO bucketed_agg_reg_test VALUES
        (1, 'a', 10),
        (2, 'b', 20),
        (3, 'a', 30),
        (4, 'b', 40),
        (5, 'a', 50),
        (1, 'c', 60),
        (2, 'c', 70),
        (3, 'b', 80),
        (4, 'c', 90),
        (5, 'b', 100);
    """

    // ============================================================
    // Test 1: Positive — single-BE, bucketed enabled
    //          EXPLAIN should contain BUCKETED AGGREGATE
    // ============================================================
    sql "set be_number_for_test=1"
    sql "set enable_bucketed_hash_agg = true;"

    String query = "SELECT grp, SUM(val) FROM bucketed_agg_reg_test GROUP BY grp;"
    explain {
        sql("${query}")
        contains("BUCKETED AGGREGATE")
    }

    // Shape plan should show one-phase: hashAgg[GLOBAL] → shuffle → scan (no LOCAL)
    qt_bucketed_shape """explain shape plan
    ${query}
    """

    // Verify correct results
    order_qt_bucketed_result """
    SELECT grp, SUM(val) FROM bucketed_agg_reg_test GROUP BY grp ORDER BY grp;
    """

    // ============================================================
    // Test 2: Negative — be_number=3 (multi-BE), bucketed enabled
    //          Must NOT use bucketed agg, must fall back to two-phase
    // ============================================================
    sql "set be_number_for_test=3"
    sql "set enable_bucketed_hash_agg = true;"

    explain {
        sql("${query}")
        notContains("BUCKETED AGGREGATE")
    }

    // Shape plan should show two-phase: hashAgg[GLOBAL] → shuffle → hashAgg[LOCAL] → scan
    qt_multi_be_shape """explain shape plan
    ${query}
    """

    // Results must match the single-BE bucketed result
    order_qt_multi_be_result """
    SELECT grp, SUM(val) FROM bucketed_agg_reg_test GROUP BY grp ORDER BY grp;
    """

    // ============================================================
    // Test 3: Negative — bucketed disabled
    //          Must fall back to two-phase
    // ============================================================
    sql "set be_number_for_test=1"
    sql "set enable_bucketed_hash_agg = false;"

    explain {
        sql("${query}")
        notContains("BUCKETED AGGREGATE")
    }

    order_qt_disabled_result """
    SELECT grp, SUM(val) FROM bucketed_agg_reg_test GROUP BY grp ORDER BY grp;
    """

    // ============================================================
    // Test 4: Negative — scalar aggregation (no GROUP BY)
    //          Bucketed agg does not apply
    // ============================================================
    sql "set be_number_for_test=1"
    sql "set enable_bucketed_hash_agg = true;"

    String scalarQuery = "SELECT SUM(val) FROM bucketed_agg_reg_test;"
    explain {
        sql("${scalarQuery}")
        notContains("BUCKETED AGGREGATE")
    }

    order_qt_no_group_by_result """
    SELECT SUM(val) FROM bucketed_agg_reg_test;
    """

    // ============================================================
    // Negative — query cache enabled
    //   The query cache point is the LOCAL aggregate above the scan, which
    //   the fused bucketed aggregate does not have, so the regular plan
    //   must be kept.
    // ============================================================
    sql "set be_number_for_test=1"
    sql "set enable_bucketed_hash_agg = true;"
    sql "set enable_query_cache = true;"
    explain {
        sql("${query}")
        notContains("BUCKETED AGGREGATE")
    }
    sql "set enable_query_cache = false;"
    explain {
        sql("${query}")
        contains("BUCKETED AGGREGATE")
    }

    // ============================================================
    // Test 5: COUNT(DISTINCT) + GROUP BY — results must be correct
    // ============================================================
    sql "set be_number_for_test=1"
    sql "set enable_bucketed_hash_agg = true;"
    sql """
        INSERT INTO bucketed_agg_reg_test VALUES
        (6, 'a', 110),
        (7, 'c', 120),
        (8, 'b', 130);
    """

    order_qt_count_distinct_result """
    SELECT grp, COUNT(DISTINCT id), SUM(val)
    FROM bucketed_agg_reg_test
    GROUP BY grp
    ORDER BY grp;
    """

    // The dedup aggregate of a mixed DISTINCT / non-DISTINCT query is a one-phase
    // GLOBAL(INPUT_TO_RESULT) aggregate whose non-distinct functions run in
    // INPUT_TO_BUFFER mode, so the translator keeps it on the regular
    // AggregationNode path. The regulator and the cost model must not favour
    // that one-phase shape either: the plan has to deduplicate locally before
    // the exchange instead of shuffling the raw scan rows.
    String mixedDistinctQuery = """
    SELECT grp, STDDEV_POP(DISTINCT id), SUM(val)
    FROM bucketed_agg_reg_test
    GROUP BY grp
    """
    explain {
        sql(mixedDistinctQuery)
        notContains("BUCKETED AGGREGATE")
    }
    qt_mixed_distinct_shape """explain shape plan
    ${mixedDistinctQuery}
    """
    order_qt_mixed_distinct_result """
    ${mixedDistinctQuery}
    ORDER BY grp
    """

    // ============================================================
    // Test 6: DISTINCT stddev/var mixed with a non-distinct aggregate.
    //         3-phase DISTINCT plans build a one-phase GLOBAL(INPUT_TO_RESULT)
    //         dedup aggregate whose non-distinct functions run in
    //         INPUT_TO_BUFFER mode (Varchar output slots). Such an aggregate
    //         must NOT be fused into BucketedAggregationNode — the bucketed
    //         node always finalizes into the tuple slot types, so writing the
    //         final DOUBLE result into the Varchar slot fails the BE
    //         result-type check ("Column type String is not compatible with
    //         data type DOUBLE").
    //         parallel_pipeline_task_num=1 makes the single-execution-instance
    //         path pick the 3-phase plan deterministically.
    // ============================================================
    sql "set be_number_for_test=1"
    sql "set enable_bucketed_hash_agg = true;"
    sql "set parallel_pipeline_task_num=1"

    order_qt_distinct_stddev_pop_result """
    SELECT STDDEV_POP(DISTINCT val), STDDEV_POP(id)
    FROM bucketed_agg_reg_test;
    """

    order_qt_distinct_stddev_samp_result """
    SELECT STDDEV_SAMP(DISTINCT val), STDDEV_SAMP(id)
    FROM bucketed_agg_reg_test;
    """

    order_qt_distinct_var_pop_result """
    SELECT VAR_POP(DISTINCT val), VAR_POP(id)
    FROM bucketed_agg_reg_test;
    """

    // ============================================================
    // Test 7: Aggregate functions with internal ORDER BY require
    //         agg_sort_infos, which BucketedAggregationNode cannot carry.
    // ============================================================
    sql "set agg_phase=1"
    sql "set be_number_for_test=1"
    sql "set enable_bucketed_hash_agg=true"
    sql "set use_one_phase_agg_for_group_concat_with_order=false"
    sql "set parallel_pipeline_task_num=2"

    sql "DROP TABLE IF EXISTS agg_group_concat_table"
    sql """
        CREATE TABLE agg_group_concat_table (
            kint INT NOT NULL,
            kbint INT NOT NULL,
            kstr STRING NOT NULL,
            kstr2 STRING NOT NULL,
            kastr ARRAY<STRING> NOT NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(kint) BUCKETS 4
        PROPERTIES('replication_num' = '1');
    """
    sql """
        INSERT INTO agg_group_concat_table VALUES
        (1, 1, 'string1', 'string3', ['s11', 's12', 's13']),
        (1, 2, 'string2', 'string1', ['s21', 's22', 's23']),
        (2, 3, 'string3', 'string2', ['s31', 's32', 's33']),
        (1, 1, 'string1', 'string3', ['s11', 's12', 's13']),
        (1, 2, 'string2', 'string1', ['s21', 's22', 's23']),
        (2, 3, 'string3', 'string2', ['s31', 's32', 's33']);
    """

    String groupConcatWithOrder = """
        SELECT multi_distinct_group_concat(kstr ORDER BY kint)
        FROM agg_group_concat_table
        GROUP BY kbint
    """
    explain {
        sql(groupConcatWithOrder)
        notContains("BUCKETED AGGREGATE")
    }
    sql(groupConcatWithOrder)

    // ============================================================
    // Test 8: COUNT must still evaluate a non-trivial argument, so the
    //         inline count path cannot hide an assert_true() failure.
    // ============================================================
    sql "set agg_phase=0"
    sql "set be_number_for_test=1"
    String countWithAssert = """
        SELECT grp, count(assert_true(val < 100, 'count argument is evaluated'))
        FROM bucketed_agg_reg_test
        GROUP BY grp
    """
    explain {
        sql(countWithAssert)
        contains("BUCKETED AGGREGATE")
    }
    test {
        sql(countWithAssert)
        exception "count argument is evaluated"
    }
    sql "set enable_bucketed_hash_agg=false"
    test {
        sql(countWithAssert)
        exception "count argument is evaluated"
    }

    // ============================================================
    // Test 9: Aggregate below a shuffle join on a non-group-by column. The
    //         join enforces a hash exchange above the aggregate; that exchange
    //         keeps the fused fragment apart from the join, so the aggregate
    //         must still be fused below it instead of paying for a raw-row
    //         exchange below a regular aggregate plus the enforcer exchange.
    // ============================================================
    sql "set enable_bucketed_hash_agg=true"
    sql "set agg_phase=1"
    sql "set be_number_for_test=1"
    sql """ DROP TABLE IF EXISTS bucketed_agg_reg_dim; """
    sql """
        CREATE TABLE bucketed_agg_reg_dim (
            k bigint,
            name varchar(20)
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        PROPERTIES('replication_num' = '1');
    """
    // 200 and 370 are the sums of grp a and grp b after the inserts above.
    sql """ INSERT INTO bucketed_agg_reg_dim VALUES (200, 'sum_a'), (370, 'sum_b'), (999, 'none'); """
    String joinChildQuery = """
        SELECT a.grp, a.s, d.name
        FROM (SELECT grp, SUM(val) AS s FROM bucketed_agg_reg_test GROUP BY grp) a
        JOIN [shuffle] bucketed_agg_reg_dim d ON a.s = d.k
    """
    explain {
        sql(joinChildQuery)
        contains("BUCKETED AGGREGATE")
    }
    order_qt_join_child_bucketed_result "${joinChildQuery}"
    sql "set enable_bucketed_hash_agg=false"
    explain {
        sql(joinChildQuery)
        notContains("BUCKETED AGGREGATE")
    }
    order_qt_join_child_regular_result "${joinChildQuery}"

    // ============================================================
    // Test 10: Aggregates that a UNION ALL consumes directly. The union absorbs
    //          its children's fragments, so these aggregates keep their own
    //          exchange (no fusion) and every fragment keeps one olap scan.
    //          Results must be correct either way.
    // ============================================================
    sql "set enable_bucketed_hash_agg=true"
    String unionChildrenQuery = """
        SELECT grp, SUM(val) AS v FROM bucketed_agg_reg_test GROUP BY grp
        UNION ALL
        SELECT grp, MAX(val) AS v FROM bucketed_agg_reg_test GROUP BY grp
    """
    order_qt_union_children_result "${unionChildrenQuery}"
    sql "set enable_bucketed_hash_agg=false"
    order_qt_union_children_regular_result "${unionChildrenQuery}"
    sql "set agg_phase=0"

    // ============================================================
    // Test 11: Window partitioned by a strict subset of the GROUP BY keys. With
    //          agg_shuffle_use_parent_key the aggregate can also shuffle its
    //          input by the window's key, which the window consumes without an
    //          exchange and the translator therefore never fuses. That
    //          alternative is a regular one-phase aggregate over a raw-row
    //          exchange and must not be exempted as a bucketed candidate: the
    //          aggregate is fused on the GROUP BY keys and feeds the window
    //          through the exchange above it.
    // ============================================================
    sql "set enable_bucketed_hash_agg=true"
    sql "set agg_shuffle_use_parent_key=true"
    String windowSubsetKeyQuery = """
        SELECT grp, val, s, SUM(s) OVER (PARTITION BY grp) AS total
        FROM (SELECT grp, val, SUM(id) AS s FROM bucketed_agg_reg_test GROUP BY grp, val) a
    """
    explain {
        sql(windowSubsetKeyQuery)
        contains("BUCKETED AGGREGATE")
    }
    order_qt_window_subset_key_bucketed_result "${windowSubsetKeyQuery}"
    sql "set enable_bucketed_hash_agg=false"
    explain {
        sql(windowSubsetKeyQuery)
        notContains("BUCKETED AGGREGATE")
    }
    order_qt_window_subset_key_regular_result "${windowSubsetKeyQuery}"
    sql "set enable_bucketed_hash_agg=true"

    // ============================================================
    // Test 12: The distribute of a one-phase aggregate must read a single olap
    //          scan pipeline to be fused. Over a nested aggregate or over a
    //          projected CTE consumer the translator keeps a regular aggregate,
    //          so the optimizer must not treat those shapes as bucketed
    //          candidates. The plan choice is asserted in
    //          BucketedAggregateTranslatorTest; here the results must match the
    //          ones without bucketed aggregation.
    // ============================================================
    String nestedAggQuery = """
        SELECT grp, SUM(m) FROM
        (SELECT grp, val, MAX(id) AS m FROM bucketed_agg_reg_test GROUP BY grp, val) a
        GROUP BY grp
    """
    String projectedCteQuery = """
        WITH c AS (SELECT grp, id, val FROM bucketed_agg_reg_test)
        SELECT g2, SUM(v) FROM (SELECT concat(grp, '_x') AS g2, val AS v FROM c) p GROUP BY g2
        UNION ALL SELECT grp, val FROM c
    """
    explain {
        sql(projectedCteQuery)
        notContains("BUCKETED AGGREGATE")
    }
    order_qt_nested_agg_bucketed_result "${nestedAggQuery}"
    order_qt_projected_cte_bucketed_result "${projectedCteQuery}"
    sql "set enable_bucketed_hash_agg=false"
    order_qt_nested_agg_regular_result "${nestedAggQuery}"
    order_qt_projected_cte_regular_result "${projectedCteQuery}"
    sql "set enable_bucketed_hash_agg=true"
}
