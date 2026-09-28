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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

class OrExpansionTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTables(
                "CREATE TABLE IF NOT EXISTS t1 (\n"
                        + "    id1 int not null,\n"
                        + "    id2 int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(id1)\n"
                        + "DISTRIBUTED BY HASH(id1) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS t2 (\n"
                        + "    id1 int not null,\n"
                        + "    id2 int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(id1)\n"
                        + "DISTRIBUTED BY HASH(id2) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS a (\n"
                        + "    k1 int not null,\n"
                        + "    k2 int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS b (\n"
                        + "    k1 int not null,\n"
                        + "    k2 int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n"
        );
    }

    // A RIGHT JOIN written in SQL is commuted to LEFT JOIN by SemiJoinCommute unless join reorder is disabled,
    // so RIGHT_OUTER_JOIN reaches OrExpansion when disable_join_reorder is true (or the join has leading /
    // distribute hint). Here we disable join reorder to keep the RIGHT JOIN, and apply OrExpansion directly
    // on the analyzed plan to cover that branch.
    //
    // a right join b on (a.k1 = b.k1 or a.k2 = b.k2)
    // => union all(
    //      a inner join b on a.k1 = b.k1,
    //      a inner join b on a.k2 = b.k2 and (a.k1 != b.k1 or ...),
    //      project(null as a.k1, null as a.k2, b.k1, b.k2)(b left anti join a on cond1 left anti join a on cond2))
    @Test
    void testOrExpandRightOuterJoin() {
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        try {
            checkOrExpandRightOuterJoin();
        } finally {
            connectContext.getSessionVariable().setDisableJoinReorder(false);
        }
    }

    private void checkOrExpandRightOuterJoin() {
        String sql = "select * from a right join b on a.k1 = b.k1 or a.k2 = b.k2";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql);

        // precondition: OrExpansion sees a nested loop RIGHT OUTER JOIN
        Plan analyzed = checker.getPlan();
        List<LogicalJoin<?, ?>> originJoins = analyzed.collectToList(LogicalJoin.class::isInstance);
        Assertions.assertEquals(1, originJoins.size(), analyzed.treeString());
        Assertions.assertTrue(originJoins.get(0).getJoinType().isRightOuterJoin(), analyzed.treeString());
        Assertions.assertTrue(originJoins.get(0).getHashJoinConjuncts().isEmpty(), analyzed.treeString());

        Plan plan = checker.applyCustom(OrExpansion.INSTANCE).printlnTree().getPlan();

        // no RIGHT OUTER JOIN is left, and every join has hash conditions
        List<LogicalJoin<?, ?>> joins = plan.collectToList(LogicalJoin.class::isInstance);
        Assertions.assertTrue(joins.stream().noneMatch(j -> j.getJoinType().isRightOuterJoin()), plan.treeString());
        Assertions.assertTrue(joins.stream().allMatch(j -> !j.getHashJoinConjuncts().isEmpty()), plan.treeString());

        List<LogicalUnion> unions = plan.collectToList(LogicalUnion.class::isInstance);
        Assertions.assertEquals(1, unions.size(), plan.treeString());
        LogicalUnion union = unions.get(0);
        // 2 inner join branches + 1 anti join branch
        Assertions.assertEquals(3, union.arity(), plan.treeString());
        Assertions.assertEquals(2, union.children().stream()
                .filter(c -> c instanceof LogicalJoin && ((LogicalJoin<?, ?>) c).getJoinType().isInnerJoin())
                .count(), plan.treeString());
        List<Plan> antiBranches = union.children().stream()
                .filter(c -> c.anyMatch(p -> p instanceof LogicalJoin
                        && ((LogicalJoin<?, ?>) p).getJoinType() == JoinType.LEFT_ANTI_JOIN))
                .collect(Collectors.toList());
        Assertions.assertEquals(1, antiBranches.size(), plan.treeString());

        // the anti branch keeps unmatched rows of b (right child):
        // output follows origin join output [a.k1, a.k2, b.k1, b.k2], a.* must be null and b.* must be real columns.
        // If RIGHT OUTER JOIN were expanded as LEFT OUTER JOIN, b.* would be null instead.
        Plan antiBranch = antiBranches.get(0);
        Assertions.assertTrue(antiBranch instanceof LogicalProject, plan.treeString());
        List<NamedExpression> projects = ((LogicalProject<?>) antiBranch).getProjects();
        Assertions.assertEquals(4, projects.size(), projects.toString());
        for (int i = 0; i < 2; i++) {
            Assertions.assertTrue(projects.get(i) instanceof Alias
                    && projects.get(i).child(0) instanceof NullLiteral, projects.toString());
        }
        for (int i = 2; i < 4; i++) {
            Assertions.assertTrue(projects.get(i) instanceof SlotReference, projects.toString());
        }
    }

    @Test
    void testOrExpand() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select t1.id1 + 1 as id from t1 join t2 on t1.id1 = t2.id1 or t1.id2 = t2.id2";
        Plan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .printlnTree()
                .getPlan();
        Assertions.assertTrue(plan instanceof LogicalCTEAnchor);
        Assertions.assertTrue(plan.child(1) instanceof LogicalCTEAnchor);
    }

    @Test
    void testOrExpandCTE() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().inlineCTEReferencedThreshold = 0;
        String sql = "with t3 as (select t1.id1 + 1 as id1, t1.id2 + 2 as id2 from t1), "
                + "t4 as (select t2.id1 + 1 as id1, t2.id2 + 2 as id2  from t2) "
                + "select t3.id1 from "
                + "(select id1, id2 from t3 group by id1, id2) t3 "
                + " join "
                + "(select id1, id2 from t4 group by id1, id2) t4  "
                + "on t3.id1 = t4.id1 or t3.id2 = t4.id2";
        Plan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .printlnTree()
                .getPlan();
        Assertions.assertTrue(plan instanceof LogicalCTEAnchor);
        Assertions.assertTrue(plan.child(1) instanceof LogicalCTEAnchor);
        Assertions.assertTrue(plan.child(1).child(1) instanceof LogicalCTEAnchor);
        Assertions.assertTrue(plan.child(1).child(1).anyMatch(x -> x instanceof LogicalCTEConsumer));
        Assertions.assertTrue(plan.child(1).child(1).child(1) instanceof LogicalCTEAnchor);
        Assertions.assertTrue(plan.child(1).child(1).child(1)
                .anyMatch(x -> x instanceof LogicalCTEConsumer));
    }
}
