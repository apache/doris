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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Push down join when one child is LogicalAssertNumRows through SetOperation.
 *
 * Example:
 * select a, b
 * from (select a, b from t1 union all select a, b from t2)
 * where a > (select x from t3)
 *
 * The subquery can be pushed down to both t1 and t2 using CTE to avoid
 * evaluating the subquery multiple times.
 *
 * <pre>
 * Before:
 *     topJoin(a > x)
 *       |-- SetOperation(UNION ALL)
 *       |     |-- Scan(T1)
 *       |     `-- Scan(T2)
 *       `-- LogicalAssertNumRows(output=(x, ...))
 *
 * After:
 *     CTEAnchor
 *       |-- CTEProducer(assertNumRows)
 *       |     `-- LogicalAssertNumRows(output=(x, ...))
 *       `-- SetOperation(UNION ALL)
 *             |-- topJoin(a > x)
 *             |     |-- Scan(T1)
 *             |     `-- CTEConsumer(assertNumRows)
 *             `-- topJoin(a > x)
 *                   |-- Scan(T2)
 *                   `-- CTEConsumer(assertNumRows)
 * </pre>
 */
public class PushDownOneRowJoinThroughSetOperation
        extends DefaultPlanRewriter<PushDownOneRowJoinThroughSetOperation.PushDownContext>
        implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        PushDownContext ctx = new PushDownContext(jobContext.getCascadesContext());
        plan = plan.accept(this, ctx);

        // Wrap with CTE anchors if any producers were created
        for (int i = ctx.cteProducerList.size() - 1; i >= 0; i--) {
            LogicalCTEProducer<? extends Plan> producer = ctx.cteProducerList.get(i);
            plan = new LogicalCTEAnchor<>(producer.getCteId(), producer, plan);
        }
        return plan;
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, PushDownContext ctx) {
        join = (LogicalJoin<? extends Plan, ? extends Plan>) super.visit(join, ctx);

        if (!patternCheck(join)) {
            return join;
        }

        return pushDownAssertNumRowsJoinThroughSetOp(join, ctx);
    }

    private boolean patternCheck(LogicalJoin<? extends Plan, ? extends Plan> join) {
        // 1. Must be INNER_JOIN or CROSS_JOIN
        if (join.getJoinType() != JoinType.INNER_JOIN && join.getJoinType() != JoinType.CROSS_JOIN) {
            return false;
        }

        // 2. Right child must be LogicalAssertNumRows (asserting 1 row)
        Plan right = join.right();
        if (!isAssertOneRowEqOrProjectAssertOneRowEq(right)) {
            return false;
        }

        // 3. Left child must be SetOperation (or Project->SetOperation)
        Plan left = join.left();
        LogicalSetOperation setOperation;

        if (left instanceof LogicalSetOperation) {
            setOperation = (LogicalSetOperation) left;
        } else if (left instanceof LogicalProject && left.child(0) instanceof LogicalSetOperation) {
            setOperation = (LogicalSetOperation) left.child(0);
        } else {
            return false;
        }

        // 4. SetOperation must have at least one child, and this child is not OneRowRelation
        if (setOperation.children().isEmpty()) {
            return false;
        }

        for (Plan child : setOperation.children()) {
            if (!(child instanceof LogicalOneRowRelation)) {
                break;
            }
            return false;
        }

        // 5. Must have exactly one join condition in otherJoinConjuncts
        if (join.getHashJoinConjuncts().isEmpty()) {
            return join.getOtherJoinConjuncts().size() == 1;
        }

        return false;
    }

    private boolean isAssertOneRowEqOrProjectAssertOneRowEq(Plan plan) {
        if (plan instanceof LogicalProject) {
            plan = plan.child(0);
        }
        if (plan instanceof LogicalAssertNumRows) {
            AssertNumRowsElement assertNumRowsElement = ((LogicalAssertNumRows<?>) plan).getAssertNumRowsElement();
            if (assertNumRowsElement.getAssertion() == AssertNumRowsElement.Assertion.EQ
                    && assertNumRowsElement.getDesiredNumOfRows() == 1L) {
                return true;
            }
        }
        return false;
    }

    private Plan pushDownAssertNumRowsJoinThroughSetOp(
            LogicalJoin<? extends Plan, ? extends Plan> join, PushDownContext ctx) {
        Plan assertBranch = join.right();
        Expression condition = (Expression) join.getOtherJoinConjuncts().get(0);
        List<Alias> aliasUsedInConditionFromLeftProject = new ArrayList<>();

        LogicalSetOperation setOperation;
        boolean hasProject = false;

        // Extract SetOperation from left child (possibly through Project)
        if (join.left() instanceof LogicalProject) {
            hasProject = true;
            LogicalProject<? extends Plan> leftProject = (LogicalProject<? extends Plan>) join.left();

            // Collect aliases used in the join condition
            for (NamedExpression namedExpression : leftProject.getProjects()) {
                if (namedExpression instanceof Alias && condition.getInputSlots().contains(namedExpression.toSlot())) {
                    aliasUsedInConditionFromLeftProject.add((Alias) namedExpression);
                }
            }

            // Push down the condition expression past the project
            condition = leftProject.pushDownExpressionPastProject(condition);
            setOperation = (LogicalSetOperation) leftProject.child();
        } else {
            setOperation = (LogicalSetOperation) join.left();
        }

        // Determine if CTE is needed based on the number of non-constant children
        boolean useCTE = needCTE(setOperation);

        // Create CTE for the AssertNumRows branch only if needed
        LogicalPlan assertBranchClone = null;
        LogicalCTEProducer<? extends Plan> assertProducer = null;
        Map<Slot, Slot> assertCloneToOriginal = new HashMap<>();

        if (useCTE) {
            // Create CTE for the AssertNumRows branch to avoid repeated evaluation
            assertBranchClone = LogicalPlanDeepCopier.INSTANCE
                    .deepCopy((LogicalPlan) assertBranch, new DeepCopierContext());
            assertProducer = new LogicalCTEProducer<>(
                    ctx.cascadesContext.getStatementContext().getNextCTEId(), assertBranchClone);

            // Create mapping from cloned slots to original slots
            for (int i = 0; i < assertBranchClone.getOutput().size(); i++) {
                assertCloneToOriginal.put(assertBranchClone.getOutput().get(i), assertBranch.getOutput().get(i));
            }
        }

        // Create new children by pushing join down to each child of SetOperation
        List<Plan> newSetOpChildren = new ArrayList<>();
        for (int childIdx = 0; childIdx < setOperation.children().size(); childIdx++) {
            Plan setOpChild = setOperation.child(childIdx);

            // Push down the condition expression to this child's context
            Expression childCondition = setOperation.pushDownExpressionPastSetOperator(condition, childIdx);
            if (childCondition == null) {
                // If push down failed, use original condition
                childCondition = condition;
            }

            Plan rightChild;
            Expression rewrittenCondition;

            if (useCTE && assertProducer != null) {
                // Create CTE consumer for the AssertNumRows branch
                LogicalCTEConsumer assertConsumer = new LogicalCTEConsumer(
                        ctx.cascadesContext.getStatementContext().getNextRelationId(),
                        assertProducer.getCteId(), "", assertProducer);
                ctx.cascadesContext.putCTEIdToConsumer(assertConsumer);

                // Build slot replacement map for the condition
                Map<Slot, Slot> slotReplacement = new HashMap<>();
                for (int i = 0; i < assertConsumer.getOutput().size(); i++) {
                    Slot consumerSlot = assertConsumer.getOutput().get(i);
                    Slot producerSlot = assertProducer.getOutput().get(i);
                    Slot originalSlot = assertCloneToOriginal.get(producerSlot);
                    if (originalSlot != null) {
                        slotReplacement.put(originalSlot, consumerSlot);
                    }
                }

                // Rewrite the condition with new slots
                rewrittenCondition = childCondition.rewriteUp(
                        s -> slotReplacement.containsKey(s) ? slotReplacement.get(s) : s);
                rightChild = assertConsumer;
            } else {
                // Directly use the original assertBranch without CTE
                rewrittenCondition = childCondition;
                rightChild = assertBranch;
            }

            if (setOpChild instanceof LogicalOneRowRelation) {
                newSetOpChildren.add(setOpChild);
            } else {
                // Create a new join for each child: setOpChild JOIN rightChild
                LogicalJoin<? extends Plan, ? extends Plan> newJoin = new LogicalJoin<>(
                        join.getJoinType(),
                        ImmutableList.of(), // hashJoinConjuncts
                        ImmutableList.of(rewrittenCondition), // otherJoinConjuncts
                        setOpChild,
                        rightChild,
                        join.getJoinReorderContext());

                newSetOpChildren.add(newJoin);
            }
        }

        // Create new SetOperation with pushed-down joins
        Plan newSetOperation = setOperation.withChildren(newSetOpChildren);

        // If there was a Project on top, recreate it
        Plan result;
        if (hasProject) {
            LogicalProject<? extends Plan> leftProject = (LogicalProject<? extends Plan>) join.left();
            result = leftProject.withChildren(newSetOperation);
        } else {
            result = newSetOperation;
        }

        // Add the CTE producer to the context only if CTE is used
        if (useCTE && assertProducer != null) {
            ctx.cteProducerList.add(assertProducer);
        }

        return result;
    }

    private boolean needCTE(LogicalSetOperation setOperation) {
        return setOperation.getNonConstChildrenCount() > 1;
    }

    /**
     * Context for tracking CTE producers during the rewrite process.
     */
    static class PushDownContext {
        List<LogicalCTEProducer<? extends Plan>> cteProducerList;
        CascadesContext cascadesContext;

        public PushDownContext(CascadesContext cascadesContext) {
            this.cascadesContext = cascadesContext;
            this.cteProducerList = new ArrayList<>();
        }
    }
}
