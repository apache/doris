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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

public class SemiJoinSemiJoinTransposeProjectTest implements MemoPatternMatchSupported {
    public static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    public static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    public static final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testSemiProjectSemiCommute() {
        /*
         *     t1.name=t3.name              t1.id=t2.id
         *       topJoin                  newTopJoin
         *       /     \                   /        \
         *    project   t3        t1.name=t3.name    t2
         *    t1.name       -->    newBottomJoin
         *      |                     /    \
         * t1.id=t2.id             t1      t3
         * bottomJoin
         *   /    \
         * t1      t2
         */
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_ANTI_JOIN, Pair.of(0, 0))
                .project(ImmutableList.of(1))
                .join(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 1))
                .projectAll()
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(SemiJoinSemiJoinTransposeProject.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(logicalJoin(
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                        ).when(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)),
                                        logicalProject(logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2")))
                                ).when(join -> join.getJoinType() == JoinType.LEFT_ANTI_JOIN)
                        )
                );
    }

    @Test
    public void testSemiProjectSemiCommuteMarkJoin() {
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .markJoinWithMarkConjuncts(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .project(ImmutableList.of(0, 2))
                .markJoinWithMarkConjuncts(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 1))
                .project(ImmutableList.of(1, 2))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(SemiJoinSemiJoinTransposeProject.INSTANCE.build())
                .matchesExploration(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                                ).when(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)
                                        ).when(project -> project.getProjects().size() == 2),
                                        logicalProject(logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2")))
                                ).when(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)
                        )
                );
    }

    @Test
    public void testEliminatedTopSemiOverRetainedBottomMarkCommutes() {
        /*
         * after mark-join elimination of the TOP IN apply (a plain SemiIN, no mark) over a
         * RETAINED bottom EXISTS mark join, the transpose can move the SemiIN BELOW the mark
         * semi join:
         *
         *   topSemi(SemiIN)                 newTopSemi(MarkSemiExists)
         *     /       \                       /       \
         * abProject   t3                abProject     t2(e1, assert_true)
         *   |                              |
         * bottomMark(t1 semi t2)    newBottomSemi(SemiIN)
         *   /    \                      /    \
         * t1     t2                   t1     t3
         *
         * this is exactly the shape that can suppress a NoneMovableFunction (assert_true) in
         * the bottom mark join's RHS (t2): rows of t1 absent from t3 are pruned by the
         * SemiIN before the mark semi join evaluates its RHS, so the assertion no longer
         * runs on them. the lower apply's sensitive plan is therefore NOT safe just because
         * it is below the target in the initial apply stack.
         */
        // bottom RETAINED mark join: t1 left semi t2 with a mark, hashConjuncts (t1#0 = t2#0)
        LogicalPlan bottomMark = new LogicalPlanBuilder(scan1)
                .markJoinWithMarkConjuncts(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .build();
        // abProject exposes all t1 slots plus the mark slot
        LogicalPlan abProject = new LogicalPlanBuilder(bottomMark)
                .project(ImmutableList.of(0, 1, 2))
                .build();
        // top ELIMINATED IN: t1 left semi t3 without a mark, hashConjuncts (t1#0 = t3#0)
        LogicalPlan topJoin = new LogicalPlanBuilder(abProject)
                .join(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .projectAll()
                .build();
        // the transpose fires and swaps the two semi joins: the retained mark semi join
        // (t1 semi t2) becomes the top and the plain SemiIN (t1 semi t3) becomes the bottom
        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(SemiJoinSemiJoinTransposeProject.INSTANCE.build())
                .matchesExploration(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                                ).when(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)
                                        ),
                                        logicalProject(logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2")))
                                ).when(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)
                        )
                );
    }

    @Test
    public void testRejectedWhenBottomSemiRightSubtreeSensitive() {
        /*
         * the transpose must be rejected when the bottom semi join's right subtree contains a
         * NoneMovableFunction (assert_true): the transpose would move that subtree above the
         * new bottom semi join (which prunes rows), so the assertion would be evaluated on
         * fewer rows and its required error suppressed - the exact behavior a retained mark
         * join preserves. the plan keeps the original order.
         */
        // bottom retained mark join over a sensitive right subtree (assert_true in a filter)
        LogicalPlan sensitiveRhs = new LogicalFilter<>(ImmutableSet.of(
                new AssertTrue(BooleanLiteral.TRUE, new VarcharLiteral("bad"))), scan2);
        LogicalPlan bottomMark = new LogicalPlanBuilder(scan1)
                .markJoinWithMarkConjuncts(sensitiveRhs, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .build();
        LogicalPlan abProject = new LogicalPlanBuilder(bottomMark)
                .project(ImmutableList.of(0, 1, 2))
                .build();
        LogicalPlan topJoin = new LogicalPlanBuilder(abProject)
                .join(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .projectAll()
                .build();
        // the transpose is rejected, so the plan keeps the original order
        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(SemiJoinSemiJoinTransposeProject.INSTANCE.build())
                .matches(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalOlapScan().when(s -> s.getTable().getName().equals("t1")),
                                                        logicalFilter(logicalOlapScan().when(s -> s.getTable().getName().equals("t2")))
                                                ).when(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)
                                        ),
                                        logicalOlapScan().when(s -> s.getTable().getName().equals("t3"))
                                ).when(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)
                        )
                );
    }

    @Test
    public void testSemiProjectSemiCommuteRejectedWhenTopJoinReferencesBottomMarkSlot() {
        /*
         * the transpose must be rejected when the bottom semi join is a mark join and the
         * top semi join references the mark slot in its conjuncts. otherwise the transposed
         * plan would move the conjuncts that reference the mark slot to a join whose
         * children don't output the mark slot, which fails physical planning with
         * "slot not from children".
         *
         *        topJoin(references mark)        the transpose is rejected, the plan
         *        /       \                       keeps the original order:
         *    abProject    t3                     topJoin
         *      |                                  /      \
         * bottomMarkJoin(t1 anti t2)        abProject   t3
         *    /      \                          |
         *   t1      t2                   bottomMarkJoin
         *                                       /      \
         *                                      t1      t2
         */
        // bottom mark join: t1 left anti t2, markJoinConjuncts = (t1#0 = t2#0),
        // output = [t1#0, t1#1, markSlot]
        LogicalPlan bottomMarkJoin = new LogicalPlanBuilder(scan1)
                .markJoinWithMarkConjuncts(scan2, JoinType.LEFT_ANTI_JOIN, Pair.of(0, 0))
                .build();
        // project exposes [t1#0, markSlot]
        LogicalPlan abProject = new LogicalPlanBuilder(bottomMarkJoin)
                .project(ImmutableList.of(0, 2))
                .build();
        // top anti join on t3 whose other conjunct references the mark slot of the bottom
        // mark join, this is exactly the plan shape that used to trigger the bug
        Slot markSlot = abProject.getOutput().get(1);
        LogicalPlan topJoin = new LogicalPlanBuilder(abProject)
                .join(scan3, JoinType.LEFT_ANTI_JOIN, ImmutableList.of(), ImmutableList.of(markSlot))
                .projectAll()
                .build();
        // the transpose is rejected, so the plan keeps the original order and the
        // mark join still produces the mark slot below the top anti join
        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(SemiJoinSemiJoinTransposeProject.INSTANCE.build())
                .matches(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalOlapScan().when(s -> s.getTable().getName().equals("t1")),
                                                        logicalOlapScan().when(s -> s.getTable().getName().equals("t2"))
                                                ).when(join -> join.getJoinType() == JoinType.LEFT_ANTI_JOIN)
                                        ),
                                        logicalOlapScan().when(s -> s.getTable().getName().equals("t3"))
                                ).when(join -> join.getJoinType() == JoinType.LEFT_ANTI_JOIN)
                        )
                );
    }
}
