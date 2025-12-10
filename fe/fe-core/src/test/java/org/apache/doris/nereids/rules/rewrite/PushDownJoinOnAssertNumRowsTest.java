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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement.Assertion;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

/**
 * Test for PushDownJoinOnAssertNumRows rule.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PushDownJoinOnAssertNumRowsTest implements MemoPatternMatchSupported {

    private LogicalOlapScan t1;
    private LogicalOlapScan t2;
    private LogicalOlapScan t3;

    private List<Slot> t1Slots;
    private List<Slot> t2Slots;
    private List<Slot> t3Slots;

    /**
     * Setup test fixtures.
     */
    @BeforeAll
    final void beforeAll() {
        t1 = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student,
        ImmutableList.of(""));
        t2 = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.score,
        ImmutableList.of(""));
        t3 = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.course,
        ImmutableList.of(""));
        t1Slots = t1.getOutput();
        t2Slots = t2.getOutput();
        t3Slots = t3.getOutput();
    }

    /**
     * Test basic push down to left child scenario:
     * Before:
     * topJoin(T1.a > x)
     * |-- bottomJoin(T1.b = T2.b)
     * | |-- Scan(T1)
     * | `-- Scan(T2)
     * `-- LogicalAssertNumRows(output=(x, ...))
     *
     * After:
     * bottomJoin(T1.b = T2.b)
     * |-- topJoin(T1.a > x)
     * | |-- Scan(T1)
     * | `-- LogicalAssertNumRows(output=(x, ...))
     * `-- Scan(T2)
     */
    @Test
    void testPushDownToLeftChild() {
        // Create a one-row relation wrapped in LogicalAssertNumRows
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Create bottom join: T1 JOIN T2 on T1.id = T2.sid
        Expression bottomJoinCondition = new EqualTo(t1Slots.get(0), t2Slots.get(1));

        LogicalPlan bottomJoin = new LogicalPlanBuilder(t1)
                .join(t2, JoinType.INNER_JOIN, ImmutableList.of(bottomJoinCondition),
                                ImmutableList.of())
                .build();

        // Create top join: (T1 JOIN T2) JOIN assertNumRows on T1.age > course.id
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(bottomJoin)
                .join(assertNumRows, JoinType.INNER_JOIN, ImmutableList.of(),
                                ImmutableList.of(topJoinCondition))
                .build();

        // Apply the rule
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOnAssertNumRows())
                .matches(logicalJoin(
                                logicalJoin(
                                                        logicalOlapScan(),
                                                        logicalAssertNumRows()),
                                logicalOlapScan()));
    }

    /**
     * Test push down to right child scenario:
     * Before:
     * topJoin(T2.a > x)
     * |-- bottomJoin(T1.b = T2.b)
     * | |-- Scan(T1)
     * | `-- Scan(T2)
     * `-- LogicalAssertNumRows(output=(x, ...))
     *
     * After:
     * bottomJoin(T1.b = T2.b)
     * |-- Scan(T1)
     * `-- topJoin(T2.a > x)
     * |-- Scan(T2)
     * `-- LogicalAssertNumRows(output=(x, ...))
     */
    @Test
    void testPushDownToRightChild() {
        // Create a one-row relation wrapped in LogicalAssertNumRows
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Create bottom join: T1 JOIN T2 on T1.id = T2.sid
        Expression bottomJoinCondition = new EqualTo(t1Slots.get(0), t2Slots.get(1));

        LogicalPlan bottomJoin = new LogicalPlanBuilder(t1)
                .join(t2, JoinType.INNER_JOIN, ImmutableList.of(bottomJoinCondition),
                                ImmutableList.of())
                .build();

        // Create top join: (T1 JOIN T2) JOIN assertNumRows on T2.name > course.name
        // This references T2 (right child of bottom join) and assertNumRows
        Expression topJoinCondition = new GreaterThan(t2Slots.get(2), t3Slots.get(1));

        LogicalPlan root = new LogicalPlanBuilder(bottomJoin)
                .join(assertNumRows, JoinType.INNER_JOIN, ImmutableList.of(),
                                ImmutableList.of(topJoinCondition))
                .build();

        // Apply the rule
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOnAssertNumRows())
                .matches(logicalJoin(
                                logicalOlapScan(),
                                logicalJoin(
                                                        logicalOlapScan(),
                                                        logicalAssertNumRows())));
    }

    /**
     * Test with project node between top join and bottom join:
     * Before:
     * topJoin(alias_col > x)
     * |-- Project(T1.id, T1.age + 1 as alias_col, ...)
     * | `-- bottomJoin(T1.id = T2.sid)
     * | |-- Scan(T1)
     * | `-- Scan(T2)
     * `-- LogicalAssertNumRows(output=(x, ...))
     *
     * After:
     * Project(...)
     * |-- bottomJoin(T1.id = T2.sid)
     * |-- topJoin(T1.age + 1 > x)
     * | |-- Scan(T1)
     * | `-- LogicalAssertNumRows(output=(x, ...))
     * `-- Scan(T2)
     */
    @Test
    void testPushDownWithProjectNode() {
        // Create a one-row relation wrapped in LogicalAssertNumRows
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Create bottom join: T1 JOIN T2 on T1.id = T2.sid
        Expression bottomJoinCondition = new EqualTo(t1Slots.get(0), t2Slots.get(1));

        LogicalPlan bottomJoin = new LogicalPlanBuilder(t1)
                .join(t2, JoinType.INNER_JOIN, ImmutableList.of(bottomJoinCondition),
                                ImmutableList.of())
                .build();

        // Create project with alias: T1.age + 1 as alias_col
        Expression addExpr = new Add(t1Slots.get(1), Literal.of(1));
        Alias aliasCol = new Alias(addExpr, "alias_col");

        ImmutableList.Builder<NamedExpression> projectListBuilder = ImmutableList.builder();
        projectListBuilder.add(t1Slots.get(0)); // T1.id
        projectListBuilder.add(aliasCol); // T1.age + 1 as alias_col
        projectListBuilder.addAll(t2Slots); // All T2 columns

        LogicalProject<Plan> project = new LogicalProject<>(projectListBuilder.build(), bottomJoin);

        // Create top join: project JOIN assertNumRows on alias_col > course.id
        Expression topJoinCondition = new GreaterThan(aliasCol.toSlot(), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(project)
                .join(assertNumRows, JoinType.INNER_JOIN, ImmutableList.of(),
                                ImmutableList.of(topJoinCondition))
                .build();

        // Apply the rule
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOnAssertNumRows())
                .matches(logicalProject(
                                logicalJoin(
                                                        logicalJoin(
                                                                        logicalProject(logicalOlapScan()),
                                                                        logicalAssertNumRows()),
                                                        logicalOlapScan())));
    }

    /**
     * Test with CROSS JOIN type.
     */
    @Test
    void testWithCrossJoin() {
        // Create a one-row relation wrapped in LogicalAssertNumRows
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Create bottom cross join: T1 CROSS JOIN T2
        LogicalPlan bottomJoin = new LogicalPlanBuilder(t1)
                .join(t2, JoinType.CROSS_JOIN, ImmutableList.of(), ImmutableList.of())
                .build();

        // Create top join with condition: T1.age > course.id
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(bottomJoin)
                .join(assertNumRows, JoinType.CROSS_JOIN, ImmutableList.of(),
                                ImmutableList.of(topJoinCondition))
                .build();

        // Apply the rule
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOnAssertNumRows())
                .matches(logicalJoin(
                                logicalJoin(
                                                        logicalOlapScan(),
                                                        logicalAssertNumRows()),
                                logicalOlapScan()));
    }

    /**
     * Test that rule doesn't apply when bottom join already has assertNumRows.
     */
    @Test
    void testNoApplyWhenBottomJoinHasAssertNumRows() {
        // Create two one-row relations
        Plan oneRowRelation1 = new LogicalPlanBuilder(t2)
                .limit(1)
                .build();
        Plan oneRowRelation2 = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement1 = new AssertNumRowsElement(1, "", Assertion.EQ);
        AssertNumRowsElement assertElement2 = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows1 = new LogicalAssertNumRows<>(assertElement1, oneRowRelation1);
        LogicalAssertNumRows<Plan> assertNumRows2 = new LogicalAssertNumRows<>(assertElement2, oneRowRelation2);

        // Create bottom join that already has assertNumRows: T1 JOIN assertNumRows1
        Expression bottomJoinCondition = new GreaterThan(t1Slots.get(1), t2Slots.get(0));

        LogicalPlan bottomJoin = new LogicalPlanBuilder(t1)
                .join(assertNumRows1, JoinType.INNER_JOIN, ImmutableList.of(),
                                ImmutableList.of(bottomJoinCondition))
                .build();

        // Create top join with another assertNumRows
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(bottomJoin)
                .join(assertNumRows2, JoinType.INNER_JOIN, ImmutableList.of(),
                                ImmutableList.of(topJoinCondition))
                .build();

        // Apply the rule - should not transform because bottom join already has
        // assertNumRows
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOnAssertNumRows())
                .matches(logicalJoin(
                                logicalJoin(
                                                        logicalOlapScan(),
                                                        logicalAssertNumRows()),
                                logicalAssertNumRows()));
    }

    /**
     * Test that rule doesn't apply when there are multiple join conditions.
     */
    @Test
    void testNoApplyWithMultipleJoinConditions() {
        // Create a one-row relation wrapped in LogicalAssertNumRows
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Create bottom join: T1 JOIN T2 on T1.id = T2.sid
        Expression bottomJoinCondition = new EqualTo(t1Slots.get(0), t2Slots.get(1));

        LogicalPlan bottomJoin = new LogicalPlanBuilder(t1)
                .join(t2, JoinType.INNER_JOIN, ImmutableList.of(bottomJoinCondition),
                                ImmutableList.of())
                .build();

        // Create top join with multiple conditions
        Expression topJoinCondition1 = new GreaterThan(t1Slots.get(1), t3Slots.get(0));
        Expression topJoinCondition2 = new GreaterThan(t2Slots.get(0), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(bottomJoin)
                .join(assertNumRows, JoinType.INNER_JOIN, ImmutableList.of(),
                                ImmutableList.of(topJoinCondition1, topJoinCondition2))
                .build();

        // Apply the rule - should not transform because there are multiple join
        // conditions
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOnAssertNumRows())
                .matches(logicalJoin(
                                logicalJoin(
                                                        logicalOlapScan(),
                                                        logicalOlapScan()),
                                logicalAssertNumRows()));
    }

    /**
     * Test that rule doesn't apply with outer join types.
     */
    @Test
    void testNoApplyWithOuterJoin() {
        // Create a one-row relation wrapped in LogicalAssertNumRows
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Create bottom left outer join: T1 LEFT JOIN T2 on T1.id = T2.sid
        Expression bottomJoinCondition = new EqualTo(t1Slots.get(0), t2Slots.get(1));

        LogicalPlan bottomJoin = new LogicalPlanBuilder(t1)
                .join(t2, JoinType.LEFT_OUTER_JOIN, ImmutableList.of(bottomJoinCondition),
                                ImmutableList.of())
                .build();

        // Create top join with condition
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(bottomJoin)
                .join(assertNumRows, JoinType.INNER_JOIN, ImmutableList.of(),
                                ImmutableList.of(topJoinCondition))
                .build();

        // Apply the rule - should not transform because bottom join is outer join
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOnAssertNumRows())
                .matches(logicalJoin(
                                logicalJoin(
                                                        logicalOlapScan(),
                                                        logicalOlapScan()),
                                logicalAssertNumRows()));
    }

    /**
     * Test with assertNumRows wrapped in project.
     */
    @Test
    void testWithAssertNumRowsInProject() {
        // Create a one-row relation wrapped in LogicalAssertNumRows and then Project
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Wrap assertNumRows in a project
        LogicalProject<Plan> assertProject = new LogicalProject<>(
                ImmutableList.copyOf(assertNumRows.getOutput()), assertNumRows);

        // Create bottom join: T1 JOIN T2 on T1.id = T2.sid
        Expression bottomJoinCondition = new EqualTo(t1Slots.get(0), t2Slots.get(1));

        LogicalPlan bottomJoin = new LogicalPlanBuilder(t1)
                .join(t2, JoinType.INNER_JOIN, ImmutableList.of(bottomJoinCondition),
                                ImmutableList.of())
                .build();

        // Create top join with project-wrapped assertNumRows
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(bottomJoin)
                .join(assertProject, JoinType.INNER_JOIN, ImmutableList.of(),
                                ImmutableList.of(topJoinCondition))
                .build();

        // Apply the rule
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOnAssertNumRows())
                .matches(logicalJoin(
                                logicalJoin(
                                                        logicalOlapScan(),
                                                        logicalProject(
                                                                        logicalAssertNumRows())),
                                logicalOlapScan()));
    }

    @Test
    void testProjectAliasOnPlan() {
        PushDownJoinOnAssertNumRows rule = new PushDownJoinOnAssertNumRows();
        Slot slot = new SlotReference(new ExprId(0), "data_slot", IntegerType.INSTANCE,
                false, ImmutableList.of());
        LogicalProject childProject = new LogicalProject(ImmutableList.of(slot),
                new LogicalEmptyRelation(new RelationId(0), ImmutableList.of()));
        Alias alias = new Alias(new ExprId(1), slot);
        LogicalPlan newPlan = rule.projectAliasOnPlan(ImmutableList.of(alias), childProject);
        Assertions.assertTrue(newPlan instanceof LogicalProject
                && newPlan.getOutput().size() == 2
                && ((LogicalProject<?>) newPlan).getOutputs().get(0).equals(slot)
                && ((LogicalProject<?>) newPlan).getOutputs().get(1).equals(alias));
    }
}
