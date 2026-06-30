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
}
