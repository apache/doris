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

// Regression for GroupJoin fusion + bucketed aggregation over the join children.
//
// Bucketed aggregation (single-BE, one-phase GLOBAL aggregate over a hash shuffle) removes
// the ExchangeNode of its olap scan, because on a single BE the shuffle is free and the
// aggregate can merge buckets in memory. The translator therefore refuses bucketed fusion
// while it is visiting the children of a fragment-merging node
// (PlanTranslatorContext.isInFragmentMergeChild): that exchange is what keeps each olap scan
// in its own fragment.
//
// translateToGroupJoinNode visited both join children without establishing that context, so
// an aggregate below a fused GroupJoin took the bucketed path and dropped its exchange. Both
// olap scans then ended up inside the GroupJoin fragment, and the scan-assignment job rejected
// it with
//   Not supported multiple scan multiple OlapTable but not contains colocate join
//   or bucket shuffle join
// (EXPLAIN still succeeded, because scan assignment only happens when the SELECT runs.)
// The dropped exchange is also what enforced the hash distribution the GroupJoin's PARTITIONED
// input relies on, so falling back to a colocate/bucket-shuffle assignment was never an option.
//
// Fix: translateToGroupJoinNode brackets its child visits with
// enterFragmentMergeChild/exitFragmentMergeChild, exactly like visitPhysicalHashJoin.
suite("test_group_join_fusion_bucketed_agg") {
    sql "DROP TABLE IF EXISTS gj_bagg_left"
    sql "DROP TABLE IF EXISTS gj_bagg_right"

    sql """
        CREATE TABLE gj_bagg_left (
            id INT NOT NULL,
            k INT NULL,
            v INT NULL,
            s SMALLINT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1")
        """

    sql "CREATE TABLE gj_bagg_right LIKE gj_bagg_left"

    sql """INSERT INTO gj_bagg_left VALUES
        (1,1,10,10), (2,1,20,20), (3,2,30,30)"""
    sql """INSERT INTO gj_bagg_right VALUES
        (1,1,7,7), (2,1,11,11), (3,2,13,13)"""

    sql "ANALYZE TABLE gj_bagg_left WITH SYNC"
    sql "ANALYZE TABLE gj_bagg_right WITH SYNC"

    // be_number_for_test = 1 pins the single-BE decision, so bucketed aggregation is used on
    // any cluster; agg_phase = 1 makes the one-phase (GLOBAL + INPUT_TO_RESULT) shape that
    // bucketed fusion requires deterministic.
    def bucketedAggSetup = { ->
        sql "SET be_number_for_test = 1"
        sql "SET enable_bucketed_hash_agg = true"
        sql "SET bucketed_agg_min_input_rows = 0"
        sql "SET bucketed_agg_high_card_threshold = 1"
        sql "SET agg_phase = 1"
        sql "SET eager_aggregation_mode = 0"
        sql "SET enable_bucket_shuffle_join = false"
        sql "SET experimental_use_serial_exchange = false"
        sql "SET runtime_filter_mode = 'OFF'"
        sql "SET parallel_pipeline_task_num = 1"
        sql "SET enable_spill = false"
        sql "SET enable_sql_cache = false"
        sql "SET query_cache_force_refresh = true"
    }
    bucketedAggSetup()

    // 1. Precondition / positive control: the settings above really do produce bucketed
    //    aggregation, so case 2 is exercising the exchange removal it must suppress.
    explain {
        sql "SELECT k, SUM(v) FROM gj_bagg_left GROUP BY k ORDER BY k"
        contains("VBUCKETED AGGREGATE")
    }
    order_qt_single_table_bucketed """
        SELECT k, SUM(v) FROM gj_bagg_left GROUP BY k ORDER BY k
    """

    // 2. Regression: an aggregated sub-query on both join sides, fused into a GroupJoin.
    //    Previously the error above; now the fused plan must keep an exchange per side, i.e.
    //    no bucketed aggregation may appear anywhere below the GroupJoin.
    def bothSidesAgg = """
        SELECT a.k, SUM(a.sv), SUM(b.sv)
        FROM (SELECT k, SUM(v) AS sv FROM gj_bagg_right GROUP BY k) a
        JOIN (SELECT k, SUM(v) AS sv FROM gj_bagg_left GROUP BY k) b ON a.k = b.k
        GROUP BY a.k
        ORDER BY a.k
        """
    sql "SET experimental_enable_group_join_fusion = false"
    def bothSidesAggRef = sql bothSidesAgg
    sql "SET experimental_enable_group_join_fusion = true"
    explain {
        sql bothSidesAgg
        contains("VGROUP JOIN")
        contains("VEXCHANGE")
        notContains("VBUCKETED AGGREGATE")
    }
    order_qt_both_sides_agg_fused bothSidesAgg
    assertEquals(bothSidesAggRef, sql(bothSidesAgg))

    // 3. The shape reported in the issue: eager pre-aggregation pushed below a [shuffle] join,
    //    so both join children carry a one-phase aggregate. Correct rows are required with
    //    fusion both off and on; the structural guarantee is case 2.
    sql "SET eager_aggregation_mode = 1"
    sql "SET eager_agg_broadcast_row_count = 0"
    def eagerAggQuery = """
        SELECT l.k, COUNT(*), SUM(l.v), SUM(r.v)
        FROM gj_bagg_left l
        JOIN [shuffle] gj_bagg_right r ON l.k = r.k
        GROUP BY l.k
        ORDER BY l.k
        """
    sql "SET experimental_enable_group_join_fusion = false"
    def eagerAggRef = sql eagerAggQuery
    order_qt_eager_agg_fusion_off eagerAggQuery
    sql "SET experimental_enable_group_join_fusion = true"
    order_qt_eager_agg_fusion_on eagerAggQuery
    assertEquals(eagerAggRef, sql(eagerAggQuery))

    // Restore defaults so other suites are not affected.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET eager_aggregation_mode = -1"
    sql "SET agg_phase = 0"
    sql "SET enable_bucketed_hash_agg = false"
    sql "SET be_number_for_test = -1"
    sql "SET runtime_filter_mode = 'GLOBAL'"
}
