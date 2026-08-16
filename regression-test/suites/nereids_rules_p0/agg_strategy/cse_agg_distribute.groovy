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

suite("cse_agg_distribute") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET runtime_filter_mode=OFF"

    sql "DROP TABLE IF EXISTS cse_agg_distribute_tbl"
    sql """
        CREATE TABLE cse_agg_distribute_tbl (
            id int,
            grp varchar(20),
            a int,
            b int
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES('replication_num' = '1')
    """
    sql """ INSERT INTO cse_agg_distribute_tbl VALUES
        (1, 'g1', 1, 2),
        (2, 'g2', 3, 4),
        (3, 'g1', 5, 6),
        (4, 'g2', 7, 8),
        (5, 'g1', 9, 10)
    """

    // one-phase aggregate over a single scan: SUM(a+b) and MAX(a+b) share the
    // same argument, so the aggregate-argument CSE must extract "a+b" into a
    // project node and make both functions reference the extracted slot.
    String query = "SELECT grp, SUM(a+b), MAX(a+b) FROM cse_agg_distribute_tbl GROUP BY grp"

    // ---------------------------------------------------------------------
    // bucketed fusion path (one-phase aggregate -> distribute -> scan is
    // fused into BucketedAggregationNode): the CSE project must be preserved
    // below the fused node, i.e. BucketedAgg(sum(x), max(x)) -> Project(a+b AS x)
    // -> scan. The aggregate output must reference the extracted slot twice
    // (once for SUM, once for MAX) instead of recomputing a+b per function.
    // ---------------------------------------------------------------------
    sql "set enable_bucketed_hash_agg=true"
    sql "set bucketed_agg_min_input_rows=0"
    sql "set bucketed_agg_high_card_threshold=1.0"
    explain {
        sql("${query}")
        contains("BUCKETED AGGREGATE")
        contains("VSELECT")
        multiContains("cast(a as BIGINT) + cast(b as BIGINT))[#", 2)
    }
    order_qt_bucketed_result """${query} ORDER BY grp"""

    // ---------------------------------------------------------------------
    // plain one-phase aggregate over a distribute (aggregate is a join child,
    // so the distribute is required by the join): the CSE project must be
    // inserted below the distribute, keeping the distribution-key slots
    // intact, and both aggregates must reference the extracted slot.
    // ---------------------------------------------------------------------
    sql "set agg_phase=1"
    sql "set enable_bucketed_hash_agg=false"
    String joinQuery = """
        SELECT t1.grp, t1.s, t1.m, t2.s2, t2.m2 FROM
         (SELECT grp, SUM(a+b) s, MAX(a+b) m FROM cse_agg_distribute_tbl GROUP BY grp) t1
         JOIN (SELECT grp, SUM(a+b) s2, MAX(a+b) m2 FROM cse_agg_distribute_tbl GROUP BY grp) t2
         ON t1.grp = t2.grp
    """
    explain {
        sql("${joinQuery}")
        contains("VEXCHANGE")
        contains("VSELECT")
        multiContains("cast(a as BIGINT) + cast(b as BIGINT))[#", 4)
    }
    order_qt_one_phase_join_result """${joinQuery} ORDER BY t1.grp"""
}
