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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Objects;

class EliminateOuterJoinTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1;
    private final LogicalOlapScan scan2;

    public EliminateOuterJoinTest() throws Exception {
        // clear id so that slot id keep consistent every running
        StatementScopeIdGenerator.clear();
        scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    }

    @Test
    void testEliminateLeft() {
        testEliminateLeftHelper(JoinType.LEFT_OUTER_JOIN);
        testEliminateLeftHelper(JoinType.ASOF_LEFT_OUTER_JOIN);
    }

    @Test
    void testEliminateLeftByInPredicateOrFalse() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))  // t1.id = t2.id
                .filter(new Or(new InPredicate(scan2.getOutput().get(0),
                        ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2))), BooleanLiteral.FALSE))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateOuterJoin())
                .applyTopDown(new EliminateNotNull())
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isInnerJoin()))
                                .when(filter -> filter.getConjuncts().size() == 1)
                );
    }

    private void testEliminateLeftHelper(JoinType joinType) {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, joinType, Pair.of(0, 0))  // t1.id = t2.id
                .filter(new GreaterThan(scan2.getOutput().get(0), Literal.of(1)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyTopDown(new EliminateOuterJoin())
                .applyTopDown(new EliminateNotNull())
                .printlnTree()
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isInnerJoin()
                                        || join.getJoinType().isAsofInnerJoin()))
                                .when(filter -> filter.getConjuncts().size() == 1)
                );
    }

    @Test
    void testEliminateRight() {
        testEliminateRightHelper(JoinType.RIGHT_OUTER_JOIN);
        testEliminateRightHelper(JoinType.ASOF_RIGHT_OUTER_JOIN);
    }

    private void testEliminateRightHelper(JoinType joinType) {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, joinType, Pair.of(0, 0))  // t1.id = t2.id
                .filter(new GreaterThan(scan1.getOutput().get(0), new IntegerLiteral(1)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyTopDown(new EliminateOuterJoin())
                .applyTopDown(new EliminateNotNull())
                .printlnTree()
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isInnerJoin()
                                        || join.getJoinType().isAsofInnerJoin()))
                                .when(filter -> filter.getConjuncts().size() == 1)
                                .when(filter -> Objects.equals(filter.getConjuncts().iterator().next(),
                                        new GreaterThan(scan1.getOutput().get(0), new IntegerLiteral(1))))
                );
    }

    @Test
    void testEliminateBoth() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.FULL_OUTER_JOIN, Pair.of(0, 0))  // t1.id = t2.id
                .filter(new And(
                        new GreaterThan(scan2.getOutput().get(0), Literal.of(1)),
                        new GreaterThan(scan1.getOutput().get(0), Literal.of(1))))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyTopDown(new EliminateOuterJoin())
                .applyTopDown(new EliminateNotNull())
                .printlnTree()
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isInnerJoin())
                        ).when(filter -> filter.getConjuncts().size() == 2)
                );
    }

    /**
     * FULL OUTER -> LEFT OUTER when only the left side is null-rejecting.
     */
    @Test
    void testFullOuterDegradeToLeft() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.FULL_OUTER_JOIN, Pair.of(0, 0))
                .filter(new GreaterThan(scan1.getOutput().get(0), new IntegerLiteral(1)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateOuterJoin())
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isLeftOuterJoin()))
                );
    }

    /**
     * FULL OUTER -> RIGHT OUTER when only the right side is null-rejecting.
     */
    @Test
    void testFullOuterDegradeToRight() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.FULL_OUTER_JOIN, Pair.of(0, 0))
                .filter(new GreaterThan(scan2.getOutput().get(0), new IntegerLiteral(1)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateOuterJoin())
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isRightOuterJoin()))
                );
    }

    /**
     * LEFT OUTER stays LEFT OUTER when the filter only constrains the preserved (left) side.
     * Without the early-return guard, the rule would still rewrite the plan with redundant
     * generated IS NOT NULL markers and trigger pointless re-rewrite churn.
     */
    @Test
    void testLeftOuterNotChangedByLeftSidePredicate() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .filter(new GreaterThan(scan1.getOutput().get(0), new IntegerLiteral(1)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateOuterJoin())
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isLeftOuterJoin()))
                                .when(filter -> filter.getConjuncts().size() == 1)
                );
    }

    /**
     * `t2.b IS NULL OR t2.b > 0` is NOT null-rejecting on t2.b (the IS NULL branch keeps NULLs),
     * so LEFT OUTER must stay LEFT OUTER.
     */
    @Test
    void testLeftOuterNotEliminatedByIsNullOr() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .filter(new Or(
                        new IsNull(scan2.getOutput().get(0)),
                        new GreaterThan(scan2.getOutput().get(0), new IntegerLiteral(0))))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateOuterJoin())
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isLeftOuterJoin()))
                );
    }

    /**
     * A predicate that mixes both sides via OR (`t1.a > 0 OR t2.b > 0`) is NOT null-rejecting
     * on either side individually, because one branch may stay TRUE when the other side is NULL.
     */
    @Test
    void testLeftOuterNotEliminatedByCrossSideOr() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .filter(new Or(
                        new GreaterThan(scan1.getOutput().get(0), new IntegerLiteral(0)),
                        new GreaterThan(scan2.getOutput().get(0), new IntegerLiteral(0))))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateOuterJoin())
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin().when(join -> join.getJoinType().isLeftOuterJoin()))
                );
    }
}
