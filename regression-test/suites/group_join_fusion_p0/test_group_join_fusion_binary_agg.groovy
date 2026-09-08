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

// Regression for GroupJoin fusion + two-argument (binary) aggregate functions.
//
// Binary aggregates such as COVAR_SAMP(r.v, r.ts) / CORR(r.v, r.ts) take their
// two arguments from the joined (build) side. When several such aggregates share
// the same argument columns, Nereids hoists the implicit type-coercion casts
// (BIGINT/INT -> DOUBLE) into a Project placed between the aggregate and the
// join, and rewrites the aggregate arguments to reference the Project's freshly
// computed slots. The GroupJoin fusion path unwraps that Project and translates
// the aggregate arguments against the join children, where those slots do not
// exist, so argument translation produced null children and fragment
// serialization aborted with
//   NullPointerException: Cannot read field "type" because the return value of
//   "java.util.ArrayList.get(int)" is null
// inside GroupJoinNode.toThrift.
//
// Fix: treat an aggregate whose inputs are not all directly produced by the join
// children (i.e. that depend on an intermediate Project between the join and the
// aggregate) as not fusible, so such queries stay on the regular
// AggregationNode + HashJoinNode path. This suite asserts the binary-agg query is
// not fused and returns exactly the reference result, while a single-argument
// aggregate query of the same shape is still fused.
suite("test_group_join_fusion_binary_agg") {
    sql "DROP TABLE IF EXISTS gj_binagg_left"
    sql "DROP TABLE IF EXISTS gj_binagg_right"

    sql """
        CREATE TABLE gj_binagg_left (
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
        CREATE TABLE gj_binagg_right (
            k1 INT NULL,
            k2 INT NULL,
            v BIGINT NULL,
            s VARCHAR(64) NULL,
            ts INT NULL
        ) DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
        """

    sql """INSERT INTO gj_binagg_left VALUES
        (1,10,5,'A',1), (1,10,15,'a',2),
        (2,20,7,'B',1), (2,21,NULL,'b',2),
        (3,30,100,'C',1), (4,40,1,'D',1),
        (NULL,50,9,'N',1), (11,10,3,'X',3)
        """

    sql """INSERT INTO gj_binagg_right VALUES
        (1,10,6,'a',3), (1,10,20,'A',4),
        (2,20,8,'B',5), (2,21,5,'b',6),
        (3,30,50,'c',7), (5,50,9,'E',8),
        (NULL,50,9,'N',9), (21,10,4,'X',10)
        """

    // Keep the plan shape deterministic: shuffle INNER join, no runtime filters,
    // no sql cache so the run always exercises the same execution path.
    sql "SET runtime_filter_mode = 'OFF'"
    sql "SET enable_sql_cache = false"

    def binaryAggQuery = """
        SELECT l.k1,
               COUNT(*) AS cnt,
               COVAR_SAMP(r.v, r.ts) AS covar_v_ts,
               CORR(r.v, r.ts) AS corr_v_ts
        FROM gj_binagg_left l
        JOIN [shuffle] gj_binagg_right r ON l.k1 = r.k1
        GROUP BY l.k1
        ORDER BY l.k1
        """

    // Reference result computed with the fusion operator disabled.
    sql "SET experimental_enable_group_join_fusion = false"
    def reference = sql binaryAggQuery
    assertTrue(reference.toString().contains("[1, 4, 4.666666666666664, 1.0]"),
            "unexpected reference result: " + reference)
    assertTrue(reference.toString().contains("[2, 4, -1.0, -1.0]"),
            "unexpected reference result: " + reference)
    assertTrue(reference.toString().contains("[3, 1, 0.0, 0.0]"),
            "unexpected reference result: " + reference)

    sql "SET experimental_enable_group_join_fusion = true"

    // Binary aggregates with hoisted argument casts must NOT be fused: the fused
    // operator cannot evaluate aggregate arguments that only an intermediate
    // Project between the aggregate and the join produces, so the query must fall
    // back to the regular hash join + aggregation path.
    def binaryAggPlan = sql "EXPLAIN " + binaryAggQuery
    assertFalse(binaryAggPlan.toString().contains("VGROUP JOIN"),
            "binary-agg query must not be fused, plan: " + binaryAggPlan)

    // Fused mode must return the same rows as the reference (no NPE, correct
    // aggregate values).
    def binaryAggResult = sql binaryAggQuery
    assertEquals(reference, binaryAggResult)

    // Positive control: an aggregate whose inputs are direct join-child columns
    // (e.g. single-argument SUM over the right side) must still be fused, so this
    // gate does not disable the fusion feature entirely.
    def singleArgQuery = """
        SELECT l.k1, COUNT(*) AS cnt, SUM(r.v) AS total_v
        FROM gj_binagg_left l
        JOIN [shuffle] gj_binagg_right r ON l.k1 = r.k1
        GROUP BY l.k1
        ORDER BY l.k1
        """
    def singleArgPlan = sql "EXPLAIN " + singleArgQuery
    assertTrue(singleArgPlan.toString().contains("VGROUP JOIN"),
            "single-arg aggregate query should still fuse, plan: " + singleArgPlan)

    // Restore defaults so other suites are not affected.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET runtime_filter_mode = 'GLOBAL'"
}
