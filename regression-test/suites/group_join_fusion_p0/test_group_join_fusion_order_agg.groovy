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

// Regression for GroupJoin fusion + order-sensitive aggregate functions
// (internal ORDER BY, e.g. GROUP_CONCAT(r.s, ',' ORDER BY r.ts)).
//
// The fused GROUP JOIN operator groups rows by the shared hash key and keeps only a
// per-key local aggregate state on one side plus the other side's per-key row count,
// so it cannot reconstruct the interleaved join row order that an aggregate with an
// internal ORDER BY needs (see the TGroupJoinAggFunction thrift comment: "当前 demo
// 版本不支持带 ORDER BY 的顺序敏感聚合函数"). The BE group-join operators always pass an
// empty TSortInfo when building the aggregate evaluator, so the translated expression's
// ORDER BY column is treated as an ordinary aggregate argument and the query aborts with
//   [INTERNAL_ERROR] Agg Function group_concat(varchar(64), varchar(1)) is not implemented
// Such aggregates must stay on the regular HashJoinNode + AggregationNode path, which
// carries per-function sort infos (agg_sort_infos) and honors the ORDER BY. This suite
// reproduces that shape (shuffle INNER join + group-by key == equi-join key + ORDER BY
// inside GROUP_CONCAT over the build side) and asserts the query is not fused and
// returns exactly the reference result, while a plain aggregate query of the same shape
// is still fused.
suite("test_group_join_fusion_order_agg") {
    sql "DROP TABLE IF EXISTS gj_orderagg_left"
    sql "DROP TABLE IF EXISTS gj_orderagg_right"

    sql """
        CREATE TABLE gj_orderagg_left (
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
        CREATE TABLE gj_orderagg_right (
            k1 INT NULL,
            k2 INT NULL,
            v BIGINT NULL,
            s VARCHAR(64) NULL,
            ts INT NULL
        ) DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
        """

    sql """INSERT INTO gj_orderagg_left VALUES
        (1,10,5,'A',1), (1,10,15,'a',2),
        (2,20,7,'B',1), (2,21,NULL,'b',2),
        (3,30,100,'C',1), (4,40,1,'D',1),
        (NULL,50,9,'N',1), (11,10,3,'X',3)
        """

    sql """INSERT INTO gj_orderagg_right VALUES
        (1,10,6,'a',3), (1,10,20,'A',4),
        (2,20,8,'B',5), (2,21,5,'b',6),
        (3,30,50,'c',7), (5,50,9,'E',8),
        (NULL,50,9,'N',9), (21,10,4,'X',10)
        """

    // Keep the plan shape deterministic: shuffle INNER join, no runtime filters,
    // no sql cache so the run always exercises the same execution path.
    sql "SET runtime_filter_mode = 'OFF'"
    sql "SET enable_sql_cache = false"

    def orderByAggQuery = """
        SELECT l.k1,
               COUNT(*) AS cnt,
               GROUP_CONCAT(r.s, ',' ORDER BY r.ts) AS text_values
        FROM gj_orderagg_left l
        JOIN [shuffle] gj_orderagg_right r ON l.k1 = r.k1
        GROUP BY l.k1
        ORDER BY l.k1
        """

    // Reference result computed with the fusion operator disabled: k1=1 matches 2x2
    // rows (r.s sorted by r.ts gives 'a,a,A,A'), k1=2 matches 2x2 rows ('B,B,b,b'),
    // k1=3 matches 1x1 rows ('c'). Groups with keys only on one side (4, 11, 21) or
    // with NULL join keys never match an inner join and must not appear.
    sql "SET experimental_enable_group_join_fusion = false"
    def reference = sql orderByAggQuery
    assertTrue(reference.toString().contains("[1, 4, a,a,A,A]"),
            "unexpected reference result: " + reference)
    assertTrue(reference.toString().contains("[2, 4, B,B,b,b]"),
            "unexpected reference result: " + reference)
    assertTrue(reference.toString().contains("[3, 1, c]"),
            "unexpected reference result: " + reference)

    sql "SET experimental_enable_group_join_fusion = true"

    // An aggregate with an internal ORDER BY must NOT be fused: the fused operator
    // cannot reconstruct the join row order the ORDER BY needs and its aggregate
    // functions carry no per-function sort info, so the query must fall back to the
    // regular hash join + aggregation path.
    def orderByAggPlan = sql "EXPLAIN " + orderByAggQuery
    assertFalse(orderByAggPlan.toString().contains("VGROUP JOIN"),
            "order-by aggregate query must not be fused, plan: " + orderByAggPlan)

    // Fused mode must return the same rows as the reference (no INTERNAL_ERROR from
    // the BE group-join operator, correct order-sensitive aggregate values).
    def orderByAggResult = sql orderByAggQuery
    assertEquals(reference, orderByAggResult)

    // Positive control: an order-insensitive aggregate of the same shape (e.g.
    // COUNT(*) + SUM over the right side) must still be fused, so this gate does not
    // disable the fusion feature entirely.
    def plainAggQuery = """
        SELECT l.k1, COUNT(*) AS cnt, SUM(r.v) AS total_v
        FROM gj_orderagg_left l
        JOIN [shuffle] gj_orderagg_right r ON l.k1 = r.k1
        GROUP BY l.k1
        ORDER BY l.k1
        """
    def plainAggPlan = sql "EXPLAIN " + plainAggQuery
    assertTrue(plainAggPlan.toString().contains("VGROUP JOIN"),
            "order-insensitive aggregate query should still fuse, plan: " + plainAggPlan)

    // Restore defaults so other suites are not affected.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET runtime_filter_mode = 'GLOBAL'"
}
