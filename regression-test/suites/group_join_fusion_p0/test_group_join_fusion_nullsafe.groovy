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

// Regression for GroupJoin fusion + null-safe equal (<=>) ON conjuncts.
//
// The fused GROUP JOIN operator groups rows by the shared hash key, and the BE
// group-join node rejects EQ_FOR_NULL hash conjuncts ("GroupJoin does not support
// null-safe equal join now"), so fusing a join whose equi-join conjunct is a
// null-safe equal (l.k1 <=> r.k1) used to abort the query with that INTERNAL_ERROR.
// Such joins must stay on the regular HashJoinNode + AggregationNode path, which
// evaluates the null-safe conjunct per matched pair and keeps the null-safe
// semantics (NULL keys on both sides match each other). This suite reproduces that
// shape (shuffle INNER join + group-by key == null-safe equi-join key, with NULL
// keys present on both sides) and asserts the null-safe query is not fused and
// returns exactly the reference result, while a plain '=' query of the same shape
// is still fused.
suite("test_group_join_fusion_nullsafe") {
    sql "DROP TABLE IF EXISTS gj_nullsafe_left"
    sql "DROP TABLE IF EXISTS gj_nullsafe_right"

    sql """
        CREATE TABLE gj_nullsafe_left (
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
        CREATE TABLE gj_nullsafe_right (
            k1 INT NULL,
            k2 INT NULL,
            v BIGINT NULL,
            s VARCHAR(64) NULL,
            ts INT NULL
        ) DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
        """

    sql """INSERT INTO gj_nullsafe_left VALUES
        (1,10,5,'A',1), (1,10,15,'a',2),
        (2,20,7,'B',1), (2,21,NULL,'b',2),
        (3,30,100,'C',1), (4,40,1,'D',1),
        (NULL,50,9,'N',1), (11,10,3,'X',3)
        """

    sql """INSERT INTO gj_nullsafe_right VALUES
        (1,10,6,'a',3), (1,10,20,'A',4),
        (2,20,8,'B',5), (2,21,5,'b',6),
        (3,30,50,'c',7), (5,50,9,'E',8),
        (NULL,50,9,'N',9), (21,10,4,'X',10)
        """

    // Keep the plan shape deterministic: shuffle INNER join, no runtime filters,
    // no sql cache so the run always exercises the same execution path.
    sql "SET runtime_filter_mode = 'OFF'"
    sql "SET enable_sql_cache = false"

    def nullSafeQuery = """
        SELECT l.k1, COUNT(*) AS cnt, SUM(r.v) AS total_v
        FROM gj_nullsafe_left l
        JOIN [shuffle] gj_nullsafe_right r ON l.k1 <=> r.k1
        GROUP BY l.k1
        ORDER BY l.k1
        """

    // Reference result computed with the fusion operator disabled.
    sql "SET experimental_enable_group_join_fusion = false"
    def reference = sql nullSafeQuery

    // Null-safe equal must match NULL keys on both sides as well: the data yields
    // groups k1=NULL (1 pair, r.v=9), k1=1 (4 pairs, 52), k1=2 (4 pairs, 26) and
    // k1=3 (1 pair, 50). Before the fix the fused plan hit the BE error
    // "GroupJoin does not support null-safe equal join now".
    assertTrue(reference.toString().contains("[null, 1, 9]"),
            "unexpected reference result: " + reference)
    assertTrue(reference.toString().contains("[1, 4, 52]"),
            "unexpected reference result: " + reference)
    assertTrue(reference.toString().contains("[2, 4, 26]"),
            "unexpected reference result: " + reference)
    assertTrue(reference.toString().contains("[3, 1, 50]"),
            "unexpected reference result: " + reference)

    sql "SET experimental_enable_group_join_fusion = true"

    // A null-safe equal join must NOT be fused: the BE group-join node cannot
    // evaluate EQ_FOR_NULL hash conjuncts, so the query must fall back to the
    // regular hash join + aggregation path.
    def nullSafePlan = sql "EXPLAIN " + nullSafeQuery
    assertFalse(nullSafePlan.toString().contains("VGROUP JOIN"),
            "null-safe equal query must not be fused, plan: " + nullSafePlan)

    // The null-safe semantics must be preserved: fused result == reference result.
    def nullSafeFused = sql nullSafeQuery
    assertEquals(reference, nullSafeFused)

    // Positive control: a plain '=' query of the same shape must still be fused,
    // so the null-safe gate does not disable the fusion feature entirely.
    def equiQuery = """
        SELECT l.k1, COUNT(*) AS cnt, SUM(r.v) AS total_v
        FROM gj_nullsafe_left l
        JOIN [shuffle] gj_nullsafe_right r ON l.k1 = r.k1
        GROUP BY l.k1
        ORDER BY l.k1
        """
    def equiPlan = sql "EXPLAIN " + equiQuery
    assertTrue(equiPlan.toString().contains("VGROUP JOIN"),
            "equi-key query should still fuse, plan: " + equiPlan)

    // Restore defaults so other suites are not affected.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET runtime_filter_mode = 'GLOBAL'"
}
