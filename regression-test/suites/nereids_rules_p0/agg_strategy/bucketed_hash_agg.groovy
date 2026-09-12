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

    // Test 7 forces one-phase aggregation. Restore automatic phase selection
    // before exercising the DISTINCT-splitting and shuffle-pruning cases below.
    sql "set agg_phase=0"

    // A mixed DISTINCT plan can contain a GLOBAL/INPUT_TO_RESULT node whose
    // non-distinct aggregate still produces a serialized buffer. It must stay
    // on the regular aggregation path instead of being fused as bucketed.
    sql "set parallel_pipeline_task_num=1"
    explain {
        sql "SELECT COUNT(DISTINCT id), AVG(val) FROM bucketed_agg_reg_test;"
        notContains("BUCKETED AGGREGATE")
    }
    order_qt_mixed_distinct_avg_result """
    SELECT COUNT(DISTINCT id), AVG(val) FROM bucketed_agg_reg_test;
    """

    // The retained shuffle key below is val. Repeating val across different grp
    // values, and repeating one complete (grp, val) key, proves that bucketed
    // execution still groups by the complete key list.
    sql """
        INSERT INTO bucketed_agg_reg_test VALUES
        (1, 'a', 200),
        (1, 'a', 200),
        (2, 'a', 200),
        (1, 'b', 200);
    """

    // Post-pruning may reduce an aggregate exchange to a safe subset. Bucketed
    // fusion still hashes raw rows by the complete GROUP BY list, so the plan
    // remains eligible after that reduction.
    sql "set parallel_pipeline_task_num=2"
    sql "set enable_parallel_result_sink=true"
    sql "set enable_shuffle_key_prune=true"
    sql "set detail_shape_nodes='PhysicalDistribute'"
    sql """
        ALTER TABLE bucketed_agg_reg_test MODIFY COLUMN grp SET STATS (
            'row_count'='100000', 'ndv'='3', 'min_value'='a', 'max_value'='c',
            'avg_size'='1', 'max_size'='1', 'hot_values'='');
    """
    sql """
        ALTER TABLE bucketed_agg_reg_test MODIFY COLUMN val SET STATS (
            'row_count'='100000', 'ndv'='10000', 'min_value'='1', 'max_value'='10000',
            'avg_size'='8', 'max_size'='8', 'hot_values'='');
    """
    String prunedBucketedQuery = """
        SELECT grp, val, COUNT(*)
        FROM bucketed_agg_reg_test
        GROUP BY grp, val
    """
    explain {
        sql "shape plan ${prunedBucketedQuery}"
        contains("Hash Columns:[val]")
        notContains("Hash Columns:[grp, val]")
    }
    explain {
        sql("${prunedBucketedQuery}")
        contains("BUCKETED AGGREGATE")
    }
    order_qt_pruned_bucketed_result """
        ${prunedBucketedQuery}
        ORDER BY grp, val;
    """

    // The parent requires [grp, val], which is unrelated to the scan's natural id
    // distribution. Post-pruning may safely reduce that exchange to [val], but it
    // remains part of the aggregate's output contract and must not be consumed by
    // bucketed fusion. Disable local shuffle to exercise that contract directly.
    sql """
        ALTER TABLE bucketed_agg_reg_test MODIFY COLUMN grp SET STATS (
            'row_count'='100000', 'ndv'='10000', 'min_value'='a', 'max_value'='c',
            'avg_size'='1', 'max_size'='1', 'hot_values'='');
    """
    sql "set enable_local_shuffle=false"
    String bucketedParentQuery = """
        SELECT grp, val, id, cnt,
               SUM(cnt) OVER (PARTITION BY grp, val) AS partition_count
        FROM (
            SELECT grp, val, id, COUNT(*) AS cnt
            FROM bucketed_agg_reg_test
            GROUP BY grp, val, id
        ) grouped
    """
    qt_pruned_bucketed_parent_shape """
        EXPLAIN SHAPE PLAN
        ${bucketedParentQuery}
        ORDER BY grp, val, id;
    """
    explain {
        sql("${bucketedParentQuery} ORDER BY grp, val, id")
        notContains("BUCKETED AGGREGATE")
    }
    order_qt_pruned_bucketed_parent_result """
        ${bucketedParentQuery}
        ORDER BY grp, val, id;
    """
}
