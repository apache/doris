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
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement.Assertion;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

/**
 * Test for PushDownOneRowJoinThroughSetOperation rule.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PushDownOneRowJoinThroughSetOperationTest implements MemoPatternMatchSupported {
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
     * Test basic push down through UNION scenario with CTE:
     * Before:
     * topJoin(col > x)
     * |-- UNION
     * | |-- Scan(T1)
     * | `-- Scan(T2)
     * `-- LogicalAssertNumRows(output=(x, ...))
     *
     * After:
     * CTEAnchor
     * |-- CTEProducer(assertNumRows)
     * | `-- LogicalAssertNumRows(output=(x, ...))
     * `-- UNION
     * |-- topJoin(col > x)
     * | |-- Scan(T1)
     * | `-- CTEConsumer(assertNumRows)
     * `-- topJoin(col > x)
     * |-- Scan(T2)
     * `-- CTEConsumer(assertNumRows)
     */
    @Test
    void testPushDownThroughUnionWithCTE() {
        // Create a one-row relation wrapped in LogicalAssertNumRows
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Create UNION: T1 UNION T2 with regularChildrenOutputs
        List<List<SlotReference>> regularChildrenOutputs = ImmutableList.of(
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                ImmutableList.copyOf((List<SlotReference>) (List) t2.getOutput().subList(0, 3)));
        LogicalUnion union = new LogicalUnion(LogicalUnion.Qualifier.ALL,
                ImmutableList.copyOf(t1.getOutput().subList(0, 3)), regularChildrenOutputs,
                ImmutableList.of(), false, ImmutableList.of(t1, t2));

        // Create top join: UNION JOIN assertNumRows on T1.age > course.id
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(union)
                .join(assertNumRows, JoinType.INNER_JOIN, ImmutableList.of(),
                    ImmutableList.of(topJoinCondition))
                .project(ImmutableList.of(0, 1))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .disableNereidsRules("prune_empty_partition").rewrite().printlnTree()
                .matches(logicalCTEAnchor(
                    logicalCTEProducer(),
                    logicalUnion(
                    logicalProject(logicalJoin(
                            logicalProject(
                                            logicalOlapScan()),
                            logicalCTEConsumer())),
                    logicalProject(logicalJoin(
                            logicalProject(
                                            logicalOlapScan()),
                            logicalCTEConsumer())))));

    }

    /**
     * Test push down through INTERSECT scenario with CTE:
     * Before:
     * topJoin(col > x)
     * |-- INTERSECT
     * | |-- Scan(T1)
     * | `-- Scan(T2)
     * `-- LogicalAssertNumRows(output=(x, ...))
     *
     * After:
     * CTEAnchor
     * |-- CTEProducer(assertNumRows)
     * | `-- LogicalAssertNumRows(output=(x, ...))
     * `-- INTERSECT
     * |-- topJoin(col > x)
     * | |-- Scan(T1)
     * | `-- CTEConsumer(assertNumRows)
     * `-- topJoin(col > x)
     * |-- Scan(T2)
     * `-- CTEConsumer(assertNumRows)
     */
    @Test
    void testPushDownThroughIntersectWithCTE() {
        // Create a one-row relation wrapped in LogicalAssertNumRows
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Create INTERSECT: T1 INTERSECT T2 with regularChildrenOutputs
        List<List<SlotReference>> regularChildrenOutputs = ImmutableList.of(
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                ImmutableList.copyOf((List<SlotReference>) (List) t2.getOutput().subList(0, 3)));
        LogicalIntersect intersect = new LogicalIntersect(LogicalIntersect.Qualifier.DISTINCT,
                ImmutableList.copyOf(t1.getOutput().subList(0, 3)), regularChildrenOutputs,
                ImmutableList.of(t1, t2));

        // Create top join: INTERSECT JOIN assertNumRows on T1.age > course.id
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(intersect)
                .join(assertNumRows, JoinType.INNER_JOIN, ImmutableList.of(),
                    ImmutableList.of(topJoinCondition))
                .project(ImmutableList.of(0, 1))
                .build();

        // Apply the rule
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .disableNereidsRules("prune_empty_partition").rewrite().printlnTree()
                .matches(logicalCTEAnchor(
                    logicalCTEProducer(),
                    logicalProject(
                    logicalIntersect(
                            logicalProject(logicalJoin(
                                            logicalOlapScan(),
                                            logicalCTEConsumer())),
                            logicalProject(logicalJoin(
                                            logicalOlapScan(),
                                            logicalCTEConsumer()))))));
    }

    /**
     * Test push down through EXCEPT scenario with CTE:
     * Before:
     * topJoin(col > x)
     * |-- EXCEPT
     * | |-- Scan(T1)
     * | `-- Scan(T2)
     * `-- LogicalAssertNumRows(output=(x, ...))
     *
     * After:
     * CTEAnchor
     * |-- CTEProducer(assertNumRows)
     * | `-- LogicalAssertNumRows(output=(x, ...))
     * `-- EXCEPT
     * |-- topJoin(col > x)
     * | |-- Scan(T1)
     * | `-- CTEConsumer(assertNumRows)
     * `-- topJoin(col > x)
     * |-- Scan(T2)
     * `-- CTEConsumer(assertNumRows)
     */
    @Test
    void testPushDownThroughExceptWithCTE() {
        // Create a one-row relation wrapped in LogicalAssertNumRows
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Create EXCEPT: T1 EXCEPT T2 with regularChildrenOutputs
        List<List<SlotReference>> regularChildrenOutputs = ImmutableList.of(
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                ImmutableList.copyOf((List<SlotReference>) (List) t2.getOutput().subList(0, 3)));
        LogicalExcept except = new LogicalExcept(LogicalExcept.Qualifier.DISTINCT,
                ImmutableList.copyOf(t1.getOutput().subList(0, 3)), regularChildrenOutputs,
                ImmutableList.of(t1, t2));

        // Create top join: EXCEPT JOIN assertNumRows on T1.age > course.id
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(except)
                .join(assertNumRows, JoinType.INNER_JOIN, ImmutableList.of(),
                    ImmutableList.of(topJoinCondition))
                .project(ImmutableList.of(0, 1))
                .build();

        // Apply the rule - should use CTE because both children are non-constant
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .disableNereidsRules("prune_empty_partition").rewrite().printlnTree()
                .matches(logicalCTEAnchor(
                    logicalCTEProducer(),
                    logicalProject(logicalExcept(
                    logicalProject(logicalJoin(
                            logicalOlapScan(),
                            logicalCTEConsumer())),
                    logicalProject(logicalJoin(
                            logicalOlapScan(),
                            logicalCTEConsumer()))))));
    }

    /**
     * Test push down through EXCEPT with constant child - should NOT use CTE:
     * Before:
     * topJoin(col > x)
     * |-- EXCEPT
     * | |-- Scan(T1)
     * | `-- OneRowRelation(select 1)
     * `-- LogicalAssertNumRows(output=(x, ...))
     *
     * After (no CTE):
     * EXCEPT
     * |-- topJoin(col > x)
     * | |-- Scan(T1)
     * | `-- LogicalAssertNumRows(output=(x, ...))
     * `-- OneRowRelation(select 1)
     */
    @Test
    void testPushDownThroughExceptWithConstantChildNoCTE() {
        // Create a one-row relation wrapped in LogicalAssertNumRows
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Create constant child: OneRowRelation (select 1)
        Alias constantExpr = new Alias(new IntegerLiteral(1), "1");
        LogicalOneRowRelation constantChild = new LogicalOneRowRelation(
                PlanConstructor.getNextRelationId(),
                ImmutableList.of(constantExpr,
                    new Alias(new IntegerLiteral(1), "2"),
                    new Alias(new StringLiteral("aa"), "3")));

        // Create EXCEPT: T1 EXCEPT (select 1) with regularChildrenOutputs
        List<List<SlotReference>> regularChildrenOutputs = ImmutableList.of(
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                ImmutableList.copyOf(
                    (List<SlotReference>) (List) constantChild.getOutput().subList(0, 3)));
        LogicalExcept except = new LogicalExcept(LogicalExcept.Qualifier.DISTINCT,
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                regularChildrenOutputs, ImmutableList.of(t1, constantChild));

        // Create top join: EXCEPT JOIN assertNumRows on T1.age > course.id
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(except)
                .join(assertNumRows, JoinType.INNER_JOIN, ImmutableList.of(),
                    ImmutableList.of(topJoinCondition))
                .build();

        // Apply the rule - should NOT use CTE because only one non-constant child
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .disableNereidsRules("prune_empty_partition")
                .rewrite()
                .printlnTree()
                .matches(logicalExcept(
                    logicalProject(logicalJoin(
                    logicalOlapScan(),
                    logicalAssertNumRows())),
                    logicalOneRowRelation()));
    }

    /**
     * Test with project node between join and set operation:
     * Before:
     * topJoin(alias_col > x)
     * |-- Project(T1.id as alias_col, ...)
     * | `-- UNION
     * | |-- Scan(T1)
     * | `-- Scan(T2)
     * `-- LogicalAssertNumRows(output=(x, ...))
     *
     * After:
     * CTEAnchor
     * |-- CTEProducer(assertNumRows)
     * | `-- LogicalAssertNumRows(output=(x, ...))
     * `-- Project(...)
     * `-- UNION
     * |-- topJoin(T1.id > x)
     * | |-- Scan(T1)
     * | `-- CTEConsumer(assertNumRows)
     * `-- topJoin(T2.sid > x)
     * |-- Scan(T2)
     * `-- CTEConsumer(assertNumRows)
     */
    @Test
    void testPushDownWithProjectNode() {
        // Create a one-row relation wrapped in LogicalAssertNumRows
        Plan oneRowRelation = new LogicalPlanBuilder(t3)
                .limit(1)
                .build();

        AssertNumRowsElement assertElement = new AssertNumRowsElement(1, "", Assertion.EQ);
        LogicalAssertNumRows<Plan> assertNumRows = new LogicalAssertNumRows<>(assertElement, oneRowRelation);

        // Create UNION: T1 UNION T2 with regularChildrenOutputs
        List<List<SlotReference>> regularChildrenOutputs = ImmutableList.of(
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                ImmutableList.copyOf((List<SlotReference>) (List) t2.getOutput()).subList(0, 3));
        LogicalUnion union = new LogicalUnion(LogicalUnion.Qualifier.ALL,
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                regularChildrenOutputs,
                ImmutableList.of(), false, ImmutableList.of(t1, t2));

        // Create project: just pass through T1.id as alias
        Alias aliasCol = new Alias(t1Slots.get(0), "alias_col");
        ImmutableList.Builder<NamedExpression> projectListBuilder = ImmutableList.builder();
        projectListBuilder.add(aliasCol);

        LogicalProject<Plan> project = new LogicalProject<>(projectListBuilder.build(), union);

        // Create top join: project JOIN assertNumRows on alias_col > course.id
        Expression topJoinCondition = new GreaterThan(aliasCol.toSlot(), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(project)
                .join(assertNumRows, JoinType.INNER_JOIN, ImmutableList.of(),
                    ImmutableList.of(topJoinCondition))
                .project(ImmutableList.of(0, 1))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .disableNereidsRules("prune_empty_partition")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalCTEAnchor(
                                logicalCTEProducer(),
                                logicalUnion(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalProject(
                                                                logicalOlapScan()),
                                                        logicalCTEConsumer())),
                                        logicalProject(
                                                logicalJoin(
                                                        logicalProject(
                                                                logicalOlapScan()),
                                                        logicalCTEConsumer())))));
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

        // Create UNION: T1 UNION T2 with regularChildrenOutputs
        List<List<SlotReference>> regularChildrenOutputs = ImmutableList.of(
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                ImmutableList.copyOf((List<SlotReference>) (List) t2.getOutput().subList(0, 3)));
        LogicalUnion union = new LogicalUnion(LogicalUnion.Qualifier.ALL,
                ImmutableList.copyOf(t1.getOutput().subList(0, 3)), regularChildrenOutputs,
                ImmutableList.of(), false, ImmutableList.of(t1, t2));

        // Create cross join with condition: T1.age > course.id
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(union)
                .join(assertNumRows, JoinType.CROSS_JOIN, ImmutableList.of(),
                    ImmutableList.of(topJoinCondition))
                .project(ImmutableList.of(0, 1))
                .build();

        // Apply the rule
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .disableNereidsRules("prune_empty_partition")
                .rewrite().printlnTree()
                .matches(logicalCTEAnchor(
                    logicalCTEProducer(),
                    logicalUnion(
                    logicalProject(logicalJoin(
                            logicalProject(
                                            logicalOlapScan()),
                            logicalCTEConsumer())),
                    logicalProject(logicalJoin(
                            logicalProject(
                                            logicalOlapScan()),
                            logicalCTEConsumer())))));
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

        // Create UNION: T1 UNION T2 with regularChildrenOutputs
        List<List<SlotReference>> regularChildrenOutputs = ImmutableList.of(
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                ImmutableList.copyOf((List<SlotReference>) (List) t2.getOutput()).subList(0, 3));
        LogicalUnion union = new LogicalUnion(LogicalUnion.Qualifier.ALL,
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                regularChildrenOutputs,
                ImmutableList.of(), false, ImmutableList.of(t1, t2));

        // Create top join with multiple conditions
        Expression topJoinCondition1 = new GreaterThan(t1Slots.get(1), t3Slots.get(0));
        Expression topJoinCondition2 = new GreaterThan(t1Slots.get(0), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(union)
                .join(assertNumRows, JoinType.INNER_JOIN, ImmutableList.of(),
                    ImmutableList.of(topJoinCondition1, topJoinCondition2))
                .build();

        // Apply the rule - should not transform because there are multiple join
        // conditions
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .customRewrite(new PushDownOneRowJoinThroughSetOperation())
                .matches(logicalJoin(
                    logicalUnion(
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

        // Create UNION: T1 UNION T2 with regularChildrenOutputs
        List<List<SlotReference>> regularChildrenOutputs = ImmutableList.of(
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                ImmutableList.copyOf((List<SlotReference>) (List) t2.getOutput()).subList(0, 3));
        LogicalUnion union = new LogicalUnion(LogicalUnion.Qualifier.ALL,
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                regularChildrenOutputs,
                ImmutableList.of(), false, ImmutableList.of(t1, t2));

        // Create left outer join
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(union)
                .join(assertNumRows, JoinType.LEFT_OUTER_JOIN, ImmutableList.of(),
                    ImmutableList.of(topJoinCondition))
                .project(ImmutableList.of(0, 1))
                .build();

        // Apply the rule - should not transform because it's outer join
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .customRewrite(new PushDownOneRowJoinThroughSetOperation())
                .matches(logicalJoin(
                    logicalUnion(
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

        // Create UNION: T1 UNION T2 with regularChildrenOutputs
        List<List<SlotReference>> regularChildrenOutputs = ImmutableList.of(
                ImmutableList.copyOf((List<SlotReference>) (List) t1.getOutput().subList(0, 3)),
                ImmutableList.copyOf((List<SlotReference>) (List) t2.getOutput().subList(0, 3)));
        LogicalUnion union = new LogicalUnion(LogicalUnion.Qualifier.ALL,
                ImmutableList.copyOf(t1.getOutput().subList(0, 3)), regularChildrenOutputs,
                ImmutableList.of(), false, ImmutableList.of(t1, t2));

        // Create top join with project-wrapped assertNumRows
        Expression topJoinCondition = new GreaterThan(t1Slots.get(1), t3Slots.get(0));

        LogicalPlan root = new LogicalPlanBuilder(union)
                .join(assertProject, JoinType.INNER_JOIN, ImmutableList.of(),
                    ImmutableList.of(topJoinCondition))
                .project(ImmutableList.of(0, 1))
                .build();

        // Apply the rule
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .disableNereidsRules("prune_empty_partition").rewrite().printlnTree()
                .matches(logicalCTEAnchor(
                    logicalCTEProducer(),
                    logicalUnion(
                    logicalProject(logicalJoin(
                            logicalProject(
                                            logicalOlapScan()),
                            logicalCTEConsumer())),
                    logicalProject(logicalJoin(
                            logicalProject(
                                            logicalOlapScan()),
                            logicalCTEConsumer())))));
    }
}
