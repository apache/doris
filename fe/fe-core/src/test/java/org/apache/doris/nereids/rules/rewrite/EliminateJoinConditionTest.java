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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

class EliminateJoinConditionTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    void basicCase() {
        LogicalPlan filterFalse = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, ImmutableList.of(BooleanLiteral.TRUE),
                        ImmutableList.of(BooleanLiteral.TRUE))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), filterFalse)
                .applyTopDown(new EliminateJoinCondition())
                .matches(
                        logicalJoin().when(join -> join.getHashJoinConjuncts().size() == 0
                                && join.getOtherJoinConjuncts().size() == 0)
                );
    }

    @Test
    void eliminateInnerJoinWithFalseCondition() {
        LogicalPlan join = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, ImmutableList.of(), ImmutableList.of(BooleanLiteral.FALSE))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new EliminateJoinCondition())
                .matches(logicalEmptyRelation());
    }

    @Test
    void eliminateLeftOuterJoinWithNullCondition() {
        LogicalPlan join = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, ImmutableList.of(),
                        ImmutableList.of(NullLiteral.BOOLEAN_INSTANCE))
                .build();

        assertNullPaddedProject(join, scan1);
    }

    @Test
    void eliminateRightOuterJoinWithFalseCondition() {
        LogicalPlan join = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.RIGHT_OUTER_JOIN, ImmutableList.of(), ImmutableList.of(BooleanLiteral.FALSE))
                .build();

        assertNullPaddedProject(join, scan2);
    }

    private void assertNullPaddedProject(LogicalPlan join, LogicalPlan preservedChild) {
        List<Slot> originalOutput = join.getOutput();
        Set<Slot> preservedOutput = preservedChild.getOutputSet();

        LogicalPlan rewritten = (LogicalPlan) PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new EliminateJoinCondition())
                .getPlan();
        Assertions.assertInstanceOf(LogicalProject.class, rewritten);
        LogicalProject<?> project = (LogicalProject<?>) rewritten;
        Assertions.assertEquals(preservedChild, project.child());
        Assertions.assertEquals(originalOutput, project.getOutput());
        for (int i = 0; i < originalOutput.size(); i++) {
            NamedExpression projectExpression = project.getProjects().get(i);
            if (preservedOutput.contains(originalOutput.get(i))) {
                Assertions.assertEquals(originalOutput.get(i), projectExpression);
            } else {
                Assertions.assertInstanceOf(Alias.class, projectExpression);
                Assertions.assertInstanceOf(NullLiteral.class, projectExpression.child(0));
                Assertions.assertEquals(originalOutput.get(i).getQualifier(), projectExpression.getQualifier());
            }
        }
    }

    @Test
    void propagateNullPaddedOutputToInnerJoinInSamePass() {
        Slot scan1Slot = scan1.getOutput().get(0);
        LogicalPlan filteredScan1 = new LogicalPlanBuilder(scan1)
                .filter(new EqualTo(scan1Slot, new IntegerLiteral(1)))
                .build();
        LogicalPlan leftOuterJoin = new LogicalPlanBuilder(filteredScan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, ImmutableList.of(),
                        ImmutableList.of(new EqualTo(scan1Slot, new IntegerLiteral(2))))
                .build();
        Slot nullPaddedSlot = leftOuterJoin.getOutput().get(filteredScan1.getOutput().size());
        LogicalPlan innerJoin = new LogicalPlanBuilder(leftOuterJoin)
                .join(scan3, JoinType.INNER_JOIN, ImmutableList.of(),
                        ImmutableList.of(new EqualTo(
                                new If(BooleanLiteral.TRUE, nullPaddedSlot, nullPaddedSlot),
                                scan3.getOutput().get(0))))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), innerJoin)
                .applyCustom(new ConstantPropagation())
                .matches(logicalEmptyRelation());
    }
}
