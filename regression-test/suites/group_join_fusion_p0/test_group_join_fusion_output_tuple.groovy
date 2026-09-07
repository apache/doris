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

// Regression for the GroupJoin fusion output-tuple handling fix.
//
// When the fused GROUP JOIN plan carries final projections above the fused node
// (aliased select list under a global top-n, e.g. ORDER BY ... LIMIT 10000 while
// LIMIT > topn_opt_limit_threshold), GroupJoinNode used to shadow
// PlanNode.outputTupleDesc. That made EXPLAIN crash with an NPE and made the
// serialized TPlanNode miss output_tuple_id while carrying projections, so the BE
// aborted (Check failed: tnode.__isset.output_tuple_id). The fix keeps the tuple
// the BE materializes the raw group-join rows into separate from
// PlanNode.outputTupleDesc (see GroupJoinNode.setMaterializedTupleDesc).
suite("test_group_join_fusion_output_tuple") {
    sql "DROP TABLE IF EXISTS gj_fusion_t1"
    sql "DROP TABLE IF EXISTS gj_fusion_t2"

    sql """
        CREATE TABLE gj_fusion_t1 (
            `k` int NOT NULL,
            `d` date NOT NULL,
            `v` int NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
        """

    sql """
        CREATE TABLE gj_fusion_t2 (
            `k` int NOT NULL,
            `tag` varchar(8) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
        """

    sql """INSERT INTO gj_fusion_t1 VALUES
        (1, '2024-01-01', 10), (1, '2024-01-02', 10), (1, '2024-01-03', 11),
        (2, '2024-02-01', 20), (2, '2024-02-02', 20),
        (3, '2024-03-01', 30), (3, '2024-03-02', 31),
        (3, '2024-03-03', 31), (3, '2024-03-04', 32),
        (4, '2024-04-01', 40),
        (7, '2024-07-01', 70), (8, '2024-08-01', 80)
        """

    sql """INSERT INTO gj_fusion_t2 VALUES
        (1, 'a'), (1, 'b'), (2, 'c'), (3, 'a'), (4, 'b'), (5, 'c'), (6, 'a')
        """

    // GROUP BY key (t1.k) equals the equi-join key, INNER join forced to shuffle, and
    // LIMIT above topn_opt_limit_threshold so the final projection is attached to the
    // fused GROUP JOIN node instead of being pushed onto a top-n/merge exchange.
    def query = """
        SELECT /*+ leading(t1 shuffle t2) */
               t1.k AS gk,
               COUNT(DISTINCT t1.v) AS cnt,
               MIN(DISTINCT t1.d) AS min_d
        FROM gj_fusion_t1 t1
        INNER JOIN gj_fusion_t2 t2 ON t2.k = t1.k
        GROUP BY t1.k
        ORDER BY gk
        LIMIT 10000
        """

    // Reference result computed with the experimental fusion operator disabled.
    sql "SET experimental_enable_group_join_fusion = false"
    def reference = sql query

    sql "SET experimental_enable_group_join_fusion = true"

    // 1. The planner must fuse join + aggregate into a GROUP JOIN node, otherwise this
    //    suite does not exercise the fix.
    def plan = sql "EXPLAIN " + query
    assertTrue(plan.toString().contains("VGROUP JOIN"),
            "GroupJoin fusion did not fire, plan: " + plan)

    // 2. Executing the fused plan must return exactly the reference result (before the
    //    fix, EXPLAIN NPE'd / the BE aborted on this shape instead of returning rows).
    def fused = sql query
    assertEquals(reference, fused)

    // Record the fused result as the expected output snapshot.
    order_qt_fused_result query

    // Restore defaults so other suites are not affected.
    sql "SET experimental_enable_group_join_fusion = false"
}
