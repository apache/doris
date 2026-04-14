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

package org.apache.doris.nereids.rules.rewrite.eageraggregation;

import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

class EagerAggRewriterTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTables(
                "CREATE TABLE IF NOT EXISTS t1 (\n"
                        + "    id1 int not null,\n"
                        + "    name varchar(20)\n"
                        + ")\n"
                        + "DUPLICATE KEY(id1)\n"
                        + "DISTRIBUTED BY HASH(id1) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS t2 (\n"
                        + "    id2 int not null,\n"
                        + "    name varchar(20)\n"
                        + ")\n"
                        + "DUPLICATE KEY(id2)\n"
                        + "DISTRIBUTED BY HASH(id2) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n"
        );
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testNotPushAggCaseWhenToNullableSideOfOuterJoin() {
        connectContext.getSessionVariable().setEagerAggregationMode(1);
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        try {
            // RIGHT JOIN: agg function (case-when) references left side columns,
            // left side is nullable, should NOT be pushed below the join
            String sql = "select max(case when t1.name is not null then 'aaa' end) from t1 right join t2 on t1.id1 = t2.id2"
                    + " group by t1.id1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(logicalAggregate(), any()))
                    .printlnTree();

            // LEFT JOIN: agg function(case-when) references right side columns,
            // right side is nullable, should NOT be pushed below the join
            sql = "select max(case when t2.name is null then 'xxx' end) from t1 left join t2"
                    + " on t1.id1 = t2.id2 group by t1.id1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(any(), logicalAggregate()))
                    .printlnTree();
            // RIGHT JOIN: agg function (not-case-when) references left side columns,
            // left side is nullable, can be pushed below the join
            sql = "select max(t2.name) from t1 left join t2"
                    + " on t1.id1 = t2.id2 group by t1.id1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(logicalJoin(any(), logicalAggregate()))
                    .printlnTree();
        } finally {
            connectContext.getSessionVariable().setEagerAggregationMode(0);
        }
    }

    @Test
    void testPushDownCount() {
        // Test count pushdown: count(a) should be pushed down and
        // the top aggregation should use sum to aggregate the count results
        // Before: agg(count(name), groupby(id2))
        //            -> join(t1.id1=t2.id2)
        //                 -> t1(id1, name)
        //                 -> t2(id2)
        // After:  agg(sum(x), groupby(id2))
        //            -> join(t1.id1=t2.id2)
        //                 -> agg(count(name) as x, groupby(id1))
        //                      -> t1(id1, name)
        //                 -> t2(id2)
        connectContext.getSessionVariable().setEagerAggregationMode(1);
        try {
            String sql = "select count(t1.name), t2.id2 from t1 join t2 on t1.id1 = t2.id2 group by t2.id2";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(logicalAggregate(logicalProject(logicalJoin(logicalAggregate(), any()))))
                    .printlnTree();
        } finally {
            connectContext.getSessionVariable().setEagerAggregationMode(0);
        }
    }

    @Test
    void testNotPushDownDistinctAgg() {
        // Distinct aggregation should not be pushed down.
        connectContext.getSessionVariable().setEagerAggregationMode(1);
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        try {
            String sql = "select count(distinct t1.name), t2.id2 from t1 join t2"
                    + " on t1.id1 = t2.id2 group by t2.id2";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(logicalAggregate(), any()))
                    .nonMatch(logicalJoin(any(), logicalAggregate()))
                    .printlnTree();
        } finally {
            connectContext.getSessionVariable().setEagerAggregationMode(0);
            connectContext.getSessionVariable().setDisableJoinReorder(false);
        }
    }

    @Test
    void testPushDownCountThroughLeftJoinWrapsWithIfnull() {
        // When count(right.col) is pushed down to the right side of a LEFT JOIN,
        // the top aggregate rolls up count to sum. But when the LEFT JOIN has no
        // matching right row, the intermediate count slot becomes NULL (null-extended),
        // and sum(NULL) = NULL. The correct result should be 0 (COUNT never returns NULL).
        // Fix: wrap the rolled-up sum with ifnull(sum(x), 0).
        connectContext.getSessionVariable().setEagerAggregationMode(1);
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        try {
            String sql = "select count(t2.id2), t1.id1 from t1 left join t2"
                    + " on t1.name = t2.name group by t1.id1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(logicalProject(logicalAggregate(logicalProject(logicalJoin(any(),
                            logicalAggregate()))))
                            .when(project -> project.getProjects().stream().anyMatch(
                                    expr -> expr.toString().contains("ifnull")
                            )))
                    .printlnTree();
        } finally {
            connectContext.getSessionVariable().setEagerAggregationMode(0);
            connectContext.getSessionVariable().setDisableJoinReorder(false);
        }
    }

    @Test
    void testNotPushCountStarToNullableSideOfOuterJoin() {
        // count(*)/count(1) counts all physical rows including null-extended rows.
        // Pushing it to the nullable side loses the count of unmatched rows:
        //   original: count(*) on unmatched row = 1
        //   pushed:   ifnull(sum(NULL), 0) = 0  (wrong!)
        // So count(*)/count(1) must NOT be pushed to the nullable side.
        connectContext.getSessionVariable().setEagerAggregationMode(1);
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        try {
            // LEFT JOIN: right side (t1) is nullable, count(*) should NOT push to right
            String sql = "select count(1), t2.id2 from t2 left join t1"
                    + " on t2.name = t1.name group by t2.id2";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(any(), logicalAggregate()))
                    .printlnTree();
        } finally {
            connectContext.getSessionVariable().setEagerAggregationMode(0);
            connectContext.getSessionVariable().setDisableJoinReorder(false);
        }
    }

    @Test
    void testPushDownCountWithIfChildShouldNotDecompose() {
        // Count(If(cond, a, b)) should NOT be decomposed into Count(a) and Count(b)
        // in the SumIf path, because the replacement logic cannot match decomposed
        // Count(a)/Count(b) as sub-expressions of the original Count(If(cond, a, b)).
        // This would cause "Input slot(s) not in child's output" error.
        // Instead, Count(If(...)) should be pushed down as-is and rolled up via
        // ifnull(sum(count_if_result), 0).
        connectContext.getSessionVariable().setEagerAggregationMode(1);
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        try {
            String sql = "select count(case when t1.id1 > 3 then t1.name else 'x' end), t2.id2"
                    + " from t1 left join t2 on t1.name = t2.name group by t2.id2";
            // Should not throw "Input slot(s) not in child's output"
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .printlnTree();
        } finally {
            connectContext.getSessionVariable().setEagerAggregationMode(0);
            connectContext.getSessionVariable().setDisableJoinReorder(false);
        }
    }

    @Test
    void testNotPushCountIfIsNullToNullableSideOfOuterJoin() {
        // count(if(col IS NULL, value, NULL)) must NOT be pushed to the nullable side
        // of an outer join. The If expression (normalized from CASE WHEN) with an IS NULL
        // condition produces wrong results when pushed:
        //   - Original: for null-extended rows, col IS NULL = TRUE, count returns 1
        //   - Pushed: pre-agg count slot becomes NULL after null-extension,
        //             ifnull(sum(NULL), 0) = 0 (wrong!)
        // This test ensures If expressions get the same protection as CaseWhen.
        connectContext.getSessionVariable().setEagerAggregationMode(1);
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        try {
            // RIGHT JOIN: t1 is the nullable side (left side of RIGHT JOIN)
            // count(case when t1.name IS NULL then 'x' end) should NOT be pushed to t1
            String sql = "select count(case when t1.name is null then 'x' end), t2.id2"
                    + " from t1 right join t2 on t1.id1 = t2.id2 group by t2.id2";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(logicalAggregate(), any()))
                    .printlnTree();

            // LEFT JOIN: t2 is the nullable side (right side of LEFT JOIN)
            // count(case when t2.name IS NULL then 'y' end) should NOT be pushed to t2
            sql = "select count(case when t2.name is null then 'y' end), t1.id1"
                    + " from t1 left join t2 on t1.id1 = t2.id2 group by t1.id1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(any(), logicalAggregate()))
                    .printlnTree();
        } finally {
            connectContext.getSessionVariable().setEagerAggregationMode(0);
            connectContext.getSessionVariable().setDisableJoinReorder(false);
        }
    }

    @Test
    void testNotPushCountCaseWhenWithElseToNullableSideViaProject() {
        // When CASE WHEN has an ELSE clause (e.g. CASE WHEN cond THEN -121 ELSE 2 END),
        // NormalizeAggregate extracts if(cond, -121, 2) into a Project as a slot,
        // so the aggregate becomes count(#slot). At the agg level, hasCaseWhen=false
        // because count(#slot) contains no If/CaseWhen.
        //
        // EagerAggRewriter must recheck hasCaseWhen after substituting through the
        // Project (in createContextFromProject), otherwise count(if(cond, -121, 2))
        // is incorrectly pushed to the nullable side of an outer join.
        //
        // On null-extended rows: if(NULL_cond, -121, 2) = 2 (ELSE branch), count(2) = 1.
        // But pre-agg doesn't include null-extended rows → ifnull(sum(NULL), 0) = 0 (wrong!).
        connectContext.getSessionVariable().setEagerAggregationMode(1);
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        try {
            // RIGHT JOIN: t1 is the nullable side
            // count(case when t1.id1 > 5 then -121 else 2 end) must NOT be pushed to t1
            String sql = "select count(case when t1.id1 > 5 then -121 else 2 end), t2.id2"
                    + " from t1 right join t2 on t1.id1 = t2.id2 group by t2.id2";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(logicalAggregate(), any()))
                    .printlnTree();

            // LEFT JOIN: t2 is the nullable side
            String sql2 = "select count(case when t2.id2 > 5 then -121 else 2 end), t1.id1"
                    + " from t1 left join t2 on t1.id1 = t2.id2 group by t1.id1";
            PlanChecker.from(connectContext)
                    .analyze(sql2)
                    .rewrite()
                    .nonMatch(logicalJoin(any(), logicalAggregate()))
                    .printlnTree();
        } finally {
            connectContext.getSessionVariable().setEagerAggregationMode(0);
            connectContext.getSessionVariable().setDisableJoinReorder(false);
        }
    }

    @Test
    void testAsofJoinNotPushAgg() {
        // Ensure ASOF joins are ignored by EagerAggRewriter (no pushdown)
        connectContext.getSessionVariable().setEagerAggregationMode(1);
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        try {
            String sql = "select count(t1.name), t2.id2 from t1 ASOF JOIN t2 "
                    + "MATCH_CONDITION(cast(t1.id1 as datetime) > cast(t2.id2 as datetime)) "
                    + "on t1.id1 = t2.id2 group by t2.id2";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(any(), logicalAggregate()))
                    .printlnTree();

            sql = "select count(t1.name), t2.id2 from t1 ASOF INNER JOIN t2 "
                    + "MATCH_CONDITION(cast(t1.id1 as datetime) > cast(t2.id2 as datetime)) "
                    + "on t1.id1 = t2.id2 group by t2.id2";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(logicalAggregate(), any()))
                    .printlnTree();
        } finally {
            connectContext.getSessionVariable().setEagerAggregationMode(0);
            connectContext.getSessionVariable().setDisableJoinReorder(false);
        }
    }

    @Test
    void testNotPushAggLiteralToNullableSideOfOuterJoin() {
        // sum(literal), min(literal), max(literal) aggregate over all physical rows,
        // including null-extended rows from the outer join.
        // Pushing to the nullable side loses the contribution of unmatched rows:
        //   original: sum(2) on unmatched row = 2
        //   pushed:   sum(NULL) skips the row (wrong!)
        // So agg(literal) must NOT be pushed to the nullable side.
        connectContext.getSessionVariable().setEagerAggregationMode(1);
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        try {
            // RIGHT JOIN: t1 is the nullable side (left side of RIGHT JOIN)
            // sum(2) should NOT be pushed to t1
            String sql = "select sum(2), t2.id2 from t1 right join t2"
                    + " on t1.id1 = t2.id2 group by t2.id2";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(logicalAggregate(), any()))
                    .printlnTree();

            // LEFT JOIN: t2 is the nullable side (right side of LEFT JOIN)
            // min(1) should NOT be pushed to t2
            sql = "select min(1), t1.id1 from t1 left join t2"
                    + " on t1.id1 = t2.id2 group by t1.id1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(any(), logicalAggregate()))
                    .printlnTree();

            // RIGHT JOIN: max(3) should NOT be pushed to nullable left side
            sql = "select max(3), t2.id2 from t1 right join t2"
                    + " on t1.id1 = t2.id2 group by t2.id2";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .nonMatch(logicalJoin(logicalAggregate(), any()))
                    .printlnTree();

            // Verify agg(nullable_side_col) is still safe to push (no regression)
            // max(t1.name) references the left (nullable) side, so push is allowed
            sql = "select max(t1.name), t2.id2 from t1 right join t2"
                    + " on t1.id1 = t2.id2 group by t2.id2";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(logicalAggregate(logicalProject(logicalJoin(logicalAggregate(), any()))))
                    .printlnTree();
        } finally {
            connectContext.getSessionVariable().setEagerAggregationMode(0);
            connectContext.getSessionVariable().setDisableJoinReorder(false);
        }
    }
}
