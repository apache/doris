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
        connectContext.setDatabase("default_cluster:test");
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
}
