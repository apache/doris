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

// Regression for GroupJoin fusion + HAVING.
//
// The fused GROUP JOIN node materializes the final aggregate result per group, but
// unlike AggregationNode its BE operator never evaluates conjuncts as a HAVING
// predicate. A post-aggregation filter (HAVING) directly above the fused aggregate
// used to be folded into the node and silently dropped, so groups failing the HAVING
// condition (e.g. k1=2 with SUM(r.v)=26 <= 30) were returned anyway. The translator
// now wraps the fused GROUP JOIN node in a SelectNode when a filter sits above it, so
// the HAVING predicate is applied to the fused result. This suite reproduces that
// shape (shuffle INNER join + group-by key == equi-join key + HAVING on an aggregate
// over the build side) and asserts the fused query applies the HAVING filter and
// returns exactly the reference result, while the join + aggregate are still fused.
suite("test_group_join_fusion_having") {
    sql "DROP TABLE IF EXISTS gj_having_left"
    sql "DROP TABLE IF EXISTS gj_having_right"

    sql """
        CREATE TABLE gj_having_left (
            k1 INT NULL,
            k2 INT NULL,
            v BIGINT NULL,
            s VARCHAR(64) NULL,
            ts INT NULL
        ) DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
        """

    sql """
        CREATE TABLE gj_having_right (
            k1 INT NULL,
            k2 INT NULL,
            v BIGINT NULL,
            s VARCHAR(64) NULL,
            ts INT NULL
        ) DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
        """

    sql """INSERT INTO gj_having_left VALUES
        (1,10,5,'A',1), (1,10,15,'a',2),
        (2,20,7,'B',1), (2,21,NULL,'b',2),
        (3,30,100,'C',1), (4,40,1,'D',1),
        (NULL,50,9,'N',1), (11,10,3,'X',3)
        """

    sql """INSERT INTO gj_having_right VALUES
        (1,10,6,'a',3), (1,10,20,'A',4),
        (2,20,8,'B',5), (2,21,5,'b',6),
        (3,30,50,'c',7), (5,50,9,'E',8),
        (NULL,50,9,'N',9), (21,10,4,'X',10)
        """

    // Keep the plan shape deterministic: shuffle INNER join, no runtime filters,
    // no sql cache so the run always exercises the same execution path.
    sql "SET runtime_filter_mode = 'OFF'"
    sql "SET enable_sql_cache = false"

    def query = """
        SELECT l.k1, COUNT(*) AS cnt, SUM(r.v) AS total_v
        FROM gj_having_left l
        JOIN [shuffle] gj_having_right r ON l.k1 = r.k1
        GROUP BY l.k1
        HAVING SUM(r.v) > 30
        ORDER BY l.k1
        """

    // Reference result computed with the fusion operator disabled.
    sql "SET experimental_enable_group_join_fusion = false"
    def reference = sql query

    // For this data the HAVING filter must drop k1=2 (SUM(r.v)=26 <= 30) and keep
    // k1=1 (4/52) and k1=3 (1/50). Before the fix the fused plan silently dropped the
    // HAVING predicate and returned the spurious k1=2 group (4/26) as well.
    assertTrue(reference.toString().contains("[1, 4, 52]"),
            "unexpected reference result: " + reference)
    assertTrue(reference.toString().contains("[3, 1, 50]"),
            "unexpected reference result: " + reference)
    assertFalse(reference.toString().contains("[2, 4, 26]"),
            "HAVING must filter out k1=2, reference: " + reference)

    sql "SET experimental_enable_group_join_fusion = true"

    // The join + aggregate must still be fused, and the HAVING predicate must be
    // applied by a SelectNode above the fused node (its BE operator does not evaluate
    // conjuncts like an AggregationNode does).
    def plan = sql "EXPLAIN " + query
    assertTrue(plan.toString().contains("VGROUP JOIN"),
            "GroupJoin fusion did not fire, plan: " + plan)
    assertTrue(plan.toString().contains("VSELECT"),
            "HAVING filter node missing above the fused join, plan: " + plan)

    // The HAVING predicate must be applied: fused result == reference result.
    def fused = sql query
    assertEquals(reference, fused)

    // Restore defaults so other suites are not affected.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET runtime_filter_mode = 'GLOBAL'"
}
