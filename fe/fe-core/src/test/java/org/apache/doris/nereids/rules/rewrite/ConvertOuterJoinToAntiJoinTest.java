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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConvertOuterJoinToAntiJoinTest implements MemoPatternMatchSupported {
    private LogicalOlapScan scan1;
    private LogicalOlapScan scan2;

    @BeforeEach
    void setUp() throws Exception {
        // clear id so that slot id keep consistent every running
        ConnectContext.remove();
        StatementScopeIdGenerator.clear();
        scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    }

    @Test
    void testEliminateLeftWithProject() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))  // t1.id = t2.id
                .filter(new IsNull(scan2.getOutput().get(0)))
                .projectExprs(ImmutableList.copyOf(scan1.getOutput()))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyCustom(new ConvertOuterJoinToAntiJoin())
                .printlnTree()
                .matches(logicalJoin().when(join -> join.getJoinType().isLeftAntiJoin()));
    }

    @Test
    void testEliminateRightWithProject() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.RIGHT_OUTER_JOIN, Pair.of(0, 0))  // t1.id = t2.id
                .filter(new IsNull(scan1.getOutput().get(0)))
                .projectExprs(ImmutableList.copyOf(scan2.getOutput()))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyCustom(new ConvertOuterJoinToAntiJoin())
                .printlnTree()
                .matches(logicalJoin().when(join -> join.getJoinType().isRightAntiJoin()));
    }

    @Test
    void testNoEliminateAsofWithProject() {
        testNoEliminateAsofWithProjectHelper(JoinType.ASOF_LEFT_OUTER_JOIN);
        testNoEliminateAsofWithProjectHelper(JoinType.ASOF_RIGHT_OUTER_JOIN);
    }

    private void testNoEliminateAsofWithProjectHelper(JoinType joinType) {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, joinType, Pair.of(0, 0))  // t1.id = t2.id
                .filter(new IsNull(scan2.getOutput().get(0)))
                .projectExprs(ImmutableList.copyOf(scan1.getOutput()))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyCustom(new ConvertOuterJoinToAntiJoin())
                .printlnTree()
                .matches(logicalJoin().when(join -> join.getJoinType() == joinType));
    }

    @Test
    void testEliminateLeftWithLeftPredicate() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))  // t1.id = t2.id
                .filter(Sets.newHashSet(
                        new IsNull(scan2.getOutput().get(0)),
                        new EqualTo(scan1.getOutput().get(0), new IntegerLiteral(1)))
                )
                .projectExprs(ImmutableList.copyOf(scan1.getOutput()))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyCustom(new ConvertOuterJoinToAntiJoin())
                .printlnTree()
                .matches(logicalJoin().when(join -> join.getJoinType().isLeftAntiJoin()));
    }

    @Test
    void testNoEliminateAsofWithLeftPredicate() {
        testNoEliminateAsofWithLeftPredicateHelper(JoinType.ASOF_LEFT_OUTER_JOIN);
        testNoEliminateAsofWithLeftPredicateHelper(JoinType.ASOF_RIGHT_OUTER_JOIN);
    }

    private void testNoEliminateAsofWithLeftPredicateHelper(JoinType joinType) {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, joinType, Pair.of(0, 0))  // t1.id = t2.id
                .filter(Sets.newHashSet(
                        new IsNull(scan2.getOutput().get(0)),
                        new EqualTo(scan1.getOutput().get(0), new IntegerLiteral(1)))
                )
                .projectExprs(ImmutableList.copyOf(scan1.getOutput()))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyCustom(new ConvertOuterJoinToAntiJoin())
                .printlnTree()
                .matches(logicalJoin().when(join -> join.getJoinType() == joinType));
    }

    @Test
    void testEliminateLeftWithRightPredicate() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))  // t1.id = t2.id
                .filter(Sets.newHashSet(
                        new IsNull(scan2.getOutput().get(0)),
                        new EqualTo(scan2.getOutput().get(0), new IntegerLiteral(1)))
                )
                .projectExprs(ImmutableList.copyOf(scan1.getOutput()))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyCustom(new ConvertOuterJoinToAntiJoin())
                .printlnTree()
                .matches(logicalJoin().when(join -> join.getJoinType().isLeftAntiJoin()));
    }

    @Test
    void testNoEliminateAsofWithRightPredicate() {
        testNoEliminateAsofWithRightPredicateHelper(JoinType.ASOF_LEFT_OUTER_JOIN);
        testNoEliminateAsofWithRightPredicateHelper(JoinType.ASOF_RIGHT_OUTER_JOIN);
    }

    private void testNoEliminateAsofWithRightPredicateHelper(JoinType joinType) {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, joinType, Pair.of(0, 0))  // t1.id = t2.id
                .filter(Sets.newHashSet(
                        new IsNull(scan2.getOutput().get(0)),
                        new EqualTo(scan2.getOutput().get(0), new IntegerLiteral(1)))
                )
                .projectExprs(ImmutableList.copyOf(scan1.getOutput()))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyCustom(new ConvertOuterJoinToAntiJoin())
                .printlnTree()
                .matches(logicalJoin().when(join -> join.getJoinType() == joinType));
    }

    @Test
    void testEliminateLeftWithOrPredicate() {
        testEliminateLeftWithOrPredicateHelper(JoinType.LEFT_OUTER_JOIN);
        testEliminateLeftWithOrPredicateHelper(JoinType.ASOF_LEFT_OUTER_JOIN);
        testEliminateLeftWithOrPredicateHelper(JoinType.ASOF_RIGHT_OUTER_JOIN);
    }

    private void testEliminateLeftWithOrPredicateHelper(JoinType joinType) {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, joinType, Pair.of(0, 0))  // t1.id = t2.id
                .filter(Sets.newHashSet(
                        new IsNull(scan1.getOutput().get(0)),
                        new Or(new IsNull(scan1.getOutput().get(0)), new IsNull(scan2.getOutput().get(0))))
                )
                .projectExprs(ImmutableList.copyOf(scan1.getOutput()))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyCustom(new ConvertOuterJoinToAntiJoin())
                .printlnTree()
                .matches(logicalJoin().when(join -> join.getJoinType() == joinType));
    }

    @Test
    void testEliminateLeftWithAndPredicate() {
        testEliminateLeftWithAndPredicateHelper(JoinType.LEFT_OUTER_JOIN);
        testEliminateLeftWithAndPredicateHelper(JoinType.ASOF_LEFT_OUTER_JOIN);
        testEliminateLeftWithAndPredicateHelper(JoinType.ASOF_RIGHT_OUTER_JOIN);
    }

    private void testEliminateLeftWithAndPredicateHelper(JoinType joinType) {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, joinType, Pair.of(0, 0))  // t1.id = t2.id
                .filter(Sets.newHashSet(
                        new IsNull(scan1.getOutput().get(0)),
                        new EqualTo(scan1.getOutput().get(0), new IntegerLiteral(1)),
                        new EqualTo(scan1.getOutput().get(0), new IntegerLiteral(1)))
                )
                .project(ImmutableList.of(2, 3))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new InferFilterNotNull())
                .applyCustom(new ConvertOuterJoinToAntiJoin())
                .printlnTree()
                .matches(logicalJoin().when(join -> join.getJoinType() == joinType));
    }
}
