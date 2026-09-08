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

// Regression for GroupJoin fusion + GROUP BY key order vs equi-join key order.
//
// The fused GROUP JOIN operator groups rows by the shared hash key and materializes one
// grouping-key column per equi-join conjunct: the BE decodes the j-th key column into the
// j-th output tuple slot, which the FE creates from the aggregate's group-by expressions
// in group-by order. When the GROUP BY lists the same keys as the equi-join but in a
// different order (e.g. GROUP BY r.k2, r.k1 over an equi-join on (k1, k2)), keeping the
// join's conjunct order used to misalign the composite group keys: each returned group-by
// column was filled with the key of the conjunct at the same position, while grouping still
// happened on the hash key, so the aggregates were right but the keys came out
// swapped/zeroed/NULL (such as 335544320 instead of 20). The AlignGroupJoinConjunctOrder
// post-processor now reorders the child join's conjuncts to the GROUP BY key order before
// runtime filters are generated, so the fused operator is both used and correct - the join's
// conjunct order is the single source of truth that both the emitted plan and runtime-filter
// expr_order observe. This suite reproduces that shape (shuffle INNER join + composite
// equi-join key (k1, k2) + GROUP BY (k2, k1)) and asserts the mismatched-order query is
// still fused and returns exactly the reference result, with and without runtime filters.
suite("test_group_join_fusion_key_order") {
    sql "DROP TABLE IF EXISTS gj_keyorder_left"
    sql "DROP TABLE IF EXISTS gj_keyorder_right"

    sql """
        CREATE TABLE gj_keyorder_left (
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
        CREATE TABLE gj_keyorder_right (
            k1 INT NULL,
            k2 INT NULL,
            v BIGINT NULL,
            s VARCHAR(64) NULL,
            ts INT NULL
        ) DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
        """

    sql """INSERT INTO gj_keyorder_left VALUES
        (1,10,5,'A',1), (1,10,15,'a',2),
        (2,20,7,'B',1), (2,21,NULL,'b',2),
        (3,30,100,'C',1), (4,40,1,'D',1),
        (NULL,50,9,'N',1), (11,10,3,'X',3)
        """

    sql """INSERT INTO gj_keyorder_right VALUES
        (1,10,6,'a',3), (1,10,20,'A',4),
        (2,20,8,'B',5), (2,21,5,'b',6),
        (3,30,50,'c',7), (5,50,9,'E',8),
        (NULL,50,9,'N',9), (21,10,4,'X',10)
        """

    // Keep the plan shape deterministic: shuffle INNER join, no runtime filters,
    // no sql cache so the run always exercises the same execution path.
    sql "SET runtime_filter_mode = 'OFF'"
    sql "SET enable_sql_cache = false"

    // GROUP BY key order (k2, k1) differs from the equi-join key order (k1, k2).
    def mismatchedQuery = """
        SELECT r.k2, r.k1, COUNT(*) AS cnt, SUM(r.v) AS total_v
        FROM gj_keyorder_left l
        JOIN [shuffle] gj_keyorder_right r
          ON l.k1 = r.k1 AND l.k2 = r.k2
        GROUP BY r.k2, r.k1
        ORDER BY r.k2, r.k1
        """

    // Reference result computed with the fusion operator disabled. Inner join on
    // (k1, k2) matches (1,10) x4 with SUM(r.v)=52, (2,20) x1 with 8, (2,21) x1 with 5
    // and (3,30) x1 with 50.
    sql "SET experimental_enable_group_join_fusion = false"
    def mismatchedReference = sql mismatchedQuery
    assertTrue(mismatchedReference.toString().contains("[10, 1, 4, 52]"),
            "unexpected reference result: " + mismatchedReference)
    assertTrue(mismatchedReference.toString().contains("[20, 2, 1, 8]"),
            "unexpected reference result: " + mismatchedReference)
    assertTrue(mismatchedReference.toString().contains("[21, 2, 1, 5]"),
            "unexpected reference result: " + mismatchedReference)
    assertTrue(mismatchedReference.toString().contains("[30, 3, 1, 50]"),
            "unexpected reference result: " + mismatchedReference)

    sql "SET experimental_enable_group_join_fusion = true"

    // A GROUP BY that permutes the equi-join keys must still be fused, with the emitted
    // conjunct order aligned to the group-by order so the group keys come out right.
    def mismatchedPlan = sql "EXPLAIN " + mismatchedQuery
    assertTrue(mismatchedPlan.toString().contains("VGROUP JOIN"),
            "mismatched-key-order query should still fuse, plan: " + mismatchedPlan)

    // The reordered grouping must be applied: fused result == reference result.
    def mismatchedFused = sql mismatchedQuery
    assertEquals(mismatchedReference, mismatchedFused)

    // The same keys grouped in the equi-join key order (k1, k2) must keep fusing too.
    def alignedQuery = """
        SELECT r.k1, r.k2, COUNT(*) AS cnt, SUM(r.v) AS total_v
        FROM gj_keyorder_left l
        JOIN [shuffle] gj_keyorder_right r
          ON l.k1 = r.k1 AND l.k2 = r.k2
        GROUP BY r.k1, r.k2
        ORDER BY r.k1, r.k2
        """
    sql "SET experimental_enable_group_join_fusion = false"
    def alignedReference = sql alignedQuery
    sql "SET experimental_enable_group_join_fusion = true"
    def alignedPlan = sql "EXPLAIN " + alignedQuery
    assertTrue(alignedPlan.toString().contains("VGROUP JOIN"),
            "aligned composite-key query should still fuse, plan: " + alignedPlan)
    def alignedFused = sql alignedQuery
    assertEquals(alignedReference, alignedFused)

    // Runtime filters must also survive the conjunct reordering: runtime-filter generation
    // runs after AlignGroupJoinConjunctOrder, so each filter's expr_order (an index into the
    // join's conjunct list) already observes the reordered list, and the BE producer resolves
    // the right source key column. Runtime filters are only planned when column statistics are
    // known, so analyze first.
    sql "ANALYZE TABLE gj_keyorder_left WITH SYNC"
    sql "ANALYZE TABLE gj_keyorder_right WITH SYNC"
    sql "SET runtime_filter_mode = 'GLOBAL'"
    sql "SET experimental_enable_group_join_fusion = false"
    def rfReference = sql mismatchedQuery
    sql "SET experimental_enable_group_join_fusion = true"
    def rfPlan = sql "EXPLAIN " + mismatchedQuery
    assertTrue(rfPlan.toString().contains("runtime filters"),
            "runtime filter not generated on the fused plan, plan: " + rfPlan)
    def rfFused = sql mismatchedQuery
    assertEquals(rfReference, rfFused)

    // Restore defaults so other suites are not affected.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET runtime_filter_mode = 'GLOBAL'"
}
