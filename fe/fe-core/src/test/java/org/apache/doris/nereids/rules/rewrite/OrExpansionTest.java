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

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
                        + "PROPERTIES (\"replication_num\" = \"1\")\n"
        );
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

    @ParameterizedTest
    @EnumSource(value = JoinType.class, names = {"INNER_JOIN", "RIGHT_OUTER_JOIN", "RIGHT_ANTI_JOIN"})
    void testOrExpandCTE(JoinType joinType) {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().inlineCTEReferencedThreshold = 0;
        String sql = "with t3 as (select t1.id1 + 1 as id1, t1.id2 + 2 as id2 from t1), "
                + "t4 as (select t2.id1 + 1 as id1, t2.id2 + 2 as id2  from t2) "
                + "select t4.id1 from "
                + "(select id1, id2 from t3 group by id1, id2) t3 "
                + " " + joinType.name().replace('_', ' ') + " "
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

    @ParameterizedTest
    @EnumSource(value = JoinType.class, names = {
            "INNER_JOIN", "LEFT_OUTER_JOIN", "RIGHT_OUTER_JOIN", "FULL_OUTER_JOIN",
            "LEFT_ANTI_JOIN", "RIGHT_ANTI_JOIN"})
    void testExpansionPreservesOutput(JoinType joinType) {
        String sql = "select * from t1 " + joinType.name().replace('_', ' ') + " t2"
                + " on t1.id1 = t2.id1 or t1.id2 = t2.id2";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql);
        List<Slot> output = checker.getPlan().getOutput();
        Plan plan = checker.applyCustom(OrExpansion.INSTANCE).getPlan();
        Assertions.assertEquals(output, plan.getOutput());
        Assertions.assertEquals(output.stream().map(Slot::nullable).collect(Collectors.toList()),
                plan.getOutput().stream().map(Slot::nullable).collect(Collectors.toList()));
        List<LogicalUnion> unions = plan.collectToList(LogicalUnion.class::isInstance);
        Assertions.assertEquals(1, unions.size(), plan.treeString());
        int innerBranches = joinType.isInnerJoin() || joinType.isOuterJoin() ? 2 : 0;
        int antiBranches = joinType.isFullOuterJoin() ? 2 : (joinType.isInnerJoin() ? 0 : 1);
        Assertions.assertEquals(innerBranches + antiBranches, unions.get(0).arity(), plan.treeString());
    }

    @ParameterizedTest
    @EnumSource(value = JoinType.class, names = {"RIGHT_OUTER_JOIN", "RIGHT_ANTI_JOIN"})
    void testRightExpansionPreservesRightInput(JoinType joinType) {
        String sql = "select * from t1 " + joinType.name().replace('_', ' ') + " t2"
                + " on (t1.id1 + 1 = t2.id1 or t1.id2 = t2.id2 + 1 or t2.id1 + 2 = t1.id2)"
                + " and t1.id1 < t2.id2";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).applyCustom(OrExpansion.INSTANCE).getPlan();
        List<LogicalCTEProducer> producers = plan.collectToList(LogicalCTEProducer.class::isInstance);
        LogicalUnion union = (LogicalUnion) plan.collectToList(LogicalUnion.class::isInstance).get(0);
        Plan unmatched = union.child(union.arity() - 1);
        List<LogicalCTEConsumer> consumers = unmatched.collectToList(LogicalCTEConsumer.class::isInstance);
        // The preserved right input is read once; each OR disjunct removes matches from it.
        Assertions.assertEquals(1, consumers.stream()
                .filter(c -> c.getCteId().equals(producers.get(1).getCteId())).count(), plan.treeString());
        Assertions.assertEquals(3, consumers.stream()
                .filter(c -> c.getCteId().equals(producers.get(0).getCteId())).count(), plan.treeString());
        List<LogicalJoin> antiJoins = unmatched.collectToList(LogicalJoin.class::isInstance);
        Assertions.assertEquals(3, antiJoins.size(), plan.treeString());
        for (LogicalJoin<?, ?> antiJoin : antiJoins) {
            Assertions.assertEquals(JoinType.LEFT_ANTI_JOIN, antiJoin.getJoinType());
            Assertions.assertEquals(1, antiJoin.getHashJoinConjuncts().size());
            Assertions.assertEquals(1, antiJoin.getOtherJoinConjuncts().size());
        }
    }

    @ParameterizedTest
    @EnumSource(value = JoinType.class, names = {"RIGHT_OUTER_JOIN", "RIGHT_ANTI_JOIN"})
    void testRightExpansionRequiresHashDisjuncts(JoinType joinType) {
        String sql = "select * from t1 " + joinType.name().replace('_', ' ') + " t2"
                + " on t1.id1 = t2.id1 or t1.id2 > t2.id2";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).applyCustom(OrExpansion.INSTANCE).getPlan();
        Assertions.assertFalse(plan.anyMatch(LogicalUnion.class::isInstance), plan.treeString());
    }
}
