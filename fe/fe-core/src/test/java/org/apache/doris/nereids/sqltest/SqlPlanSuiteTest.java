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

package org.apache.doris.nereids.sqltest;

import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.rewrite.ReorderJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Merged suite: these tests only need the shared fixture provided by the base class, so they are
 * kept in one class on purpose. Every extra test class pays a full FE startup, which dominates the
 * runtime of tests this small.
 *
 * <p>Replaces the former standalone classes:
 * <ul>
 *   <li>CascadesJoinReorderTest</li>
 *   <li>InferTest</li>
 *   <li>JoinTest</li>
 *   <li>MultiJoinTest</li>
 *   <li>SortTest</li>
 * </ul>
 */
public class SqlPlanSuiteTest extends SqlTestBase {

    // -------------------------------------------------------------------------
    // from CascadesJoinReorderTest
    // -------------------------------------------------------------------------

    @Test
    void testStartThreeJoin() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // Three join
        // (n-1)! * 2^(n-1) = 8
        String sql = "SELECT * FROM T1 "
                + "JOIN T2 ON T1.id = T2.id "
                + "JOIN T3 ON T1.id = T3.id";

        int plansNumber = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .applyExploration(RuleSet.ZIG_ZAG_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.ZIG_ZAG_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.ZIG_ZAG_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.ZIG_ZAG_TREE_JOIN_REORDER)
                .plansNumber();

        Assertions.assertEquals(8, plansNumber);
    }

    @Test
    void testStartThreeJoinBushy() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // Three join
        // (n-1)! * 2^(n-1) = 8
        String sql = "SELECT * FROM T1 "
                + "JOIN T2 ON T1.id = T2.id "
                + "JOIN T3 ON T1.id = T3.id";

        int plansNumber = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .printlnAllTree()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .plansNumber();

        Assertions.assertEquals(8, plansNumber);
    }

    @Test
    void testStarFourJoinZigzag() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // Four join
        // (n-1)! * 2^(n-1) = 48
        String sql = "SELECT * FROM T1 "
                + "JOIN T2 ON T1.id = T2.id "
                + "JOIN T3 ON T1.id = T3.id "
                + "JOIN T4 ON T1.id = T4.id ";

        int plansNumber = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .plansNumber();

        Assertions.assertEquals(48, plansNumber);
    }

    @Test
    void testStarFourJoinBushy() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // Four join
        // (n-1)! * 2^(n-1) = 48
        String sql = "SELECT * FROM T1 "
                + "JOIN T2 ON T1.id = T2.id "
                + "JOIN T3 ON T1.id = T3.id "
                + "JOIN T4 ON T1.id = T4.id ";

        int plansNumber = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .plansNumber();

        Assertions.assertEquals(48, plansNumber);
    }

    @Test
    void testChainFourJoinBushy() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // Four join
        // 2^(n-1) * C(n-1) = 40
        String sql = "SELECT * FROM T1 "
                + "JOIN T2 ON T1.id = T2.id "
                + "JOIN T3 ON T2.id = T3.id "
                + "JOIN T4 ON T3.id = T4.id ";

        int plansNumber = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .plansNumber();

        Assertions.assertEquals(40, plansNumber);
    }

    @Test
    void testChainFiveJoinBushy() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // Five join
        // 2^(n-1) * C(n-1) = 224
        String sql = "SELECT * FROM T1 "
                + "JOIN T2 ON T1.id = T2.id "
                + "JOIN T3 ON T2.id = T3.id "
                + "JOIN T4 ON T3.id = T4.id "
                + "JOIN T1 T5 ON T4.ID = T5.ID";

        int plansNumber = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .plansNumber();

        Assertions.assertEquals(224, plansNumber);
    }

    // -------------------------------------------------------------------------
    // from InferTest
    // -------------------------------------------------------------------------

    @Test
    void testInferNotNullAndInferPredicates() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // Test InferNotNull, EliminateOuter, InferPredicate together
        String sql = "select * from T1 left outer join T2 on T1.id = T2.id where T2.id = 4";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        innerLogicalJoin(
                            logicalFilter().when(f -> f.getPredicate().toString().equals("(id#0 = 4)")),
                            logicalFilter().when(f -> f.getPredicate().toString().equals("(id#2 = 4)"))
                        )
                );
    }

    @Test
    void testInferNotNullFromFilterAndEliminateOuter2() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql
                = "select * from T1 right outer join T2 on T1.id = T2.id where T1.id = 4 OR (T1.id > 4 AND T2.score IS NULL)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .printlnTree()
                .matches(
                    innerLogicalJoin(
                        logicalFilter().when(
                                f -> f.getPredicate().toString().equals("(id#2 >= 4)")),
                        logicalFilter().when(
                                f -> ExpressionUtils.and(f.getConjuncts().stream()
                                        .sorted((a, b) -> a.toString().compareTo(b.toString()))
                                        .collect(Collectors.toList()))
                                        .toString().equals("(id#0 >= 4)"))
                    )

                );
    }

    @Test
    void testInferNotNullFromFilterAndEliminateOuter3() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql
                = "select * from T1 full outer join T2 on T1.id = T2.id where T1.id = 4 OR (T1.id > 4 AND T2.score IS NULL)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalFilter(
                            leftOuterLogicalJoin(
                                logicalFilter().when(
                                        f -> f.getPredicate().toString().equals("(id#0 >= 4)")),
                                logicalFilter().when(
                                        f -> f.getPredicate().toString().equals("(id#2 >= 4)")
                                )
                            )
                        ).when(f -> f.getPredicate().toString()
                                .equals("OR[(id#0 = 4),AND[(id#0 > 4),score#3 IS NULL]]"))
                );
    }

    @Test
    void testInferNotNullFromJoinAndEliminateOuter() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // Is Not Null will infer from semi join, so right outer join can be eliminated.
        String sql
                = "select * from (select T1.id from T1 right outer join T2 on T1.id = T2.id) T1 left semi join T3 on T1.id = T3.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        innerLogicalJoin(
                                logicalProject(),
                                logicalProject(leftSemiLogicalJoin())
                        )
                );
    }

    @Test
    void aggEliminateOuterJoin() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select count(T2.score) from T1 left Join T2 on T1.id = T2.id";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalAggregate(
                               logicalProject(
                                       innerLogicalJoin()
                               )
                        )
                );
    }

    // -------------------------------------------------------------------------
    // from JoinTest
    // -------------------------------------------------------------------------

    @Test
    void testJoinUsing() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "SELECT * FROM T1 JOIN T2 using (id)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new ReorderJoin())
                .matches(
                        innerLogicalJoin().when(j -> j.getHashJoinConjuncts().size() == 1)
                );
    }

    @Test
    void testColocatedJoin() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from T2 join T2 b on T2.id = b.id and T2.id = b.id;";
        PhysicalPlan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .optimize()
                .getBestPlanTree();
        // generate colocate join plan without physicalDistribute
        System.out.println(plan.treeString());
        Assertions.assertFalse(plan.anyMatch(p -> p instanceof PhysicalDistribute
                && ((PhysicalDistribute) p).getDistributionSpec() instanceof DistributionSpecHash));
        sql = "select * from T1 join T0 on T1.score = T0.score and T1.id = T0.id;";
        plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .optimize()
                .getBestPlanTree();
        // generate colocate join plan without physicalDistribute
        Assertions.assertFalse(plan.anyMatch(p -> p instanceof PhysicalDistribute
                && ((PhysicalDistribute) p).getDistributionSpec() instanceof DistributionSpecHash));
    }

    @Test
    void testDedupConjuncts() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from T1 join T2 on T1.id = T2.id and T1.id = T2.id;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        innerLogicalJoin().when(j -> j.getHashJoinConjuncts().size() == 1)
                );

        String sql1 = "select * from T1 left join T2 on T1.id = T2.id and T1.id = T2.id;";
        PlanChecker.from(connectContext)
                .analyze(sql1)
                .rewrite()
                .matches(
                        leftOuterLogicalJoin().when(j -> j.getHashJoinConjuncts().size() == 1)
                );
    }

    @Test
    void testBucketJoinWithAgg() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from "
                + "(select distinct id as cnt from T2) T1 inner join"
                + "(select distinct id as cnt from T2) T2 "
                + "on T1.cnt = T2.cnt";
        PhysicalPlan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .optimize()
                .getBestPlanTree(PhysicalProperties.ANY);
        Assertions.assertEquals(
                ShuffleType.NATURAL,
                ((DistributionSpecHash) ((PhysicalPlan) (plan.child(0).child(0)))
                        .getPhysicalProperties().getDistributionSpec()).getShuffleType()
        );
    }

    // -------------------------------------------------------------------------
    // from MultiJoinTest
    // -------------------------------------------------------------------------

    @Test
    void testMultiJoinEliminateCross() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        List<String> sqls = ImmutableList.<String>builder()
                .add("SELECT * FROM T2 LEFT JOIN T3 ON T2.id = T3.id, T1 WHERE T1.id = T2.id")
                .add("SELECT * FROM T2 LEFT JOIN T3 ON T2.id = T3.id, T1 WHERE T1.id = T2.id AND T1.score > 0")
                .add("SELECT * FROM T2 LEFT JOIN T3 ON T2.id = T3.id, T1 WHERE T1.id = T2.id AND T1.score > 0 AND T1.id + T2.id + T3.id > 0")
                .build();

        for (String sql : sqls) {
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .applyBottomUp(new ReorderJoin())
                    .matches(
                            logicalJoin(
                                    logicalJoin().whenNot(join -> join.getJoinType().isCrossJoin()),
                                    leafPlan()
                            ).whenNot(join -> join.getJoinType().isCrossJoin())
                    )
                    .printlnTree();
        }
    }

    @Test
    @Disabled
    void testEliminateBelowOuter() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // FIXME: MultiJoin And EliminateOuter
        String sql = "SELECT * FROM T1, T2 LEFT JOIN T3 ON T2.id = T3.id WHERE T1.id = T2.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new ReorderJoin())
                .printlnTree();
    }

    @Test
    void testMultiJoinExistCross() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        List<String> sqls = ImmutableList.<String>builder()
                .add("SELECT * FROM T2 LEFT SEMI JOIN T3 ON T2.id = T3.id, T1 WHERE T1.id > T2.id")
                .build();

        for (String sql : sqls) {
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .applyBottomUp(new ReorderJoin())
                    .matches(
                            logicalJoin(
                                    logicalJoin().whenNot(join -> join.getJoinType().isCrossJoin()),
                                    leafPlan()
                            ).when(join -> join.getJoinType().isCrossJoin())
                                    .whenNot(join -> join.getOtherJoinConjuncts().isEmpty())
                    )
                    .printlnTree();
        }
    }

    @Test
    void testOuterJoin() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "SELECT * FROM T1 LEFT OUTER JOIN T2 ON T1.id = T2.id, T3 WHERE T2.score > 0";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new ReorderJoin())
                .printlnTree()
                .matches(
                        crossLogicalJoin(
                                leftOuterLogicalJoin()
                                        .when(join -> join.getOtherJoinConjuncts().size() == 1),
                                logicalOlapScan()
                        )
                );
    }

    @Test
    @Disabled
    void testNoFilter() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "Select * FROM T1 INNER JOIN T2 On true";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        crossLogicalJoin()
                );
    }

    @Test
    void test() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select T1.score, T2.score from T1 inner join T2 on T1.id = T2.id where T1.score - 2 > T2.score";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalProject(
                                innerLogicalJoin()
                        )
                );

    }

    // -------------------------------------------------------------------------
    // from SortTest
    // -------------------------------------------------------------------------

    @Test
    public void testTwoPhaseSort() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from\n"
                + "(select score from T1 order by id) as t order by score\n";
        PhysicalPlan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .optimize()
                .getBestPlanTree();
        System.out.println(plan.treeString());
        Assertions.assertTrue(plan.anyMatch(e -> e instanceof PhysicalQuickSort
                && ((PhysicalQuickSort<?>) e).getSortPhase().isMerge() && e.child(0) instanceof PhysicalDistribute));
    }
}
