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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Push down join when one child is LogicalAssertNumRows.
 * select * from T1 join T2 where T1.b=T2.b and T1.a > (select x from T3 ...)
 *
 * <pre>
 * Case 1: Push to left child
 * Before:
 *     topJoin(T1.a > x)
 *       |-- Project
 *       |     `-- bottomJoin(T1.b = T2.b)
 *       |           |-- Scan(T1)
 *       |           `-- Scan(T2)
 *       `-- LogicalAssertNumRows(output=(x, ...))
 *
 * After:
 *     Project
 *       |-- topJoin(T1.b = T2.b)
 *               |-- bottomJoin(T1.a > x)
 *                   |-- Scan(T1)
 *                   `-- LogicalAssertNumRows(output=(x, ...))
 *               `-- Scan(T2)
 *
 * Case 2: Push to right child
 * Before:
 *     topJoin(T2.a > x)
 *       |-- Project
 *       |     `-- bottomJoin(T1.b = T2.b)
 *       |           |-- Scan(T1)
 *       |           `-- Scan(T2)
 *       `-- LogicalAssertNumRows(output=(x, ...))
 *
 * After:
 *     Project
 *       |-- topJoin(T1.b = T2.b)
 *              |--Scan(T1)
 *             `-- bottomJoin(T2.a > x)
 *                   |-- Scan(T2)
 *                   `-- LogicalAssertNumRows(output=(x, ...))
 * </pre>
 */
public class PushDownJoinOnAssertNumRows extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin()
                .when(topJoin -> pattenCheck(topJoin))
                .then(topJoin -> pushDownAssertNumRowsJoin(topJoin))
                .toRule(RuleType.PUSH_DOWN_JOIN_ON_ASSERT_NUM_ROWS);
    }

    private boolean pattenCheck(LogicalJoin topJoin) {
        // 1. right is LogicalAssertNumRows or LogicalProject->LogicalAssertNumRows
        // 2. left is join or project->join
        // 3. only one join condition.
        if (topJoin.getJoinType() != JoinType.INNER_JOIN && topJoin.getJoinType() != JoinType.CROSS_JOIN) {
            return false;
        }
        LogicalJoin bottomJoin;
        Plan left = topJoin.left();
        Plan right = topJoin.right();
        if (!isAssertOneRowEqOrProjectAssertOneRowEq(right)) {
            return false;
        }
        if (left instanceof LogicalJoin) {
            bottomJoin = (LogicalJoin) left;
        } else if (left instanceof LogicalProject && left.child(0) instanceof LogicalJoin) {
            bottomJoin = (LogicalJoin) left.child(0);
        } else {
            return false;
        }

        if (bottomJoin.getJoinType() != JoinType.INNER_JOIN && bottomJoin.getJoinType() != JoinType.CROSS_JOIN) {
            return false;
        }

        if (joinOnAssertOneRowEq(bottomJoin)) {
            return false;
        }

        if (topJoin.getHashJoinConjuncts().isEmpty()) {
            return topJoin.getOtherJoinConjuncts().size() == 1;
        }
        return false;
    }

    private boolean isAssertOneRowEqOrProjectAssertOneRowEq(Plan plan) {
        if (plan instanceof LogicalProject) {
            plan = plan.child(0);
        }
        if (plan instanceof LogicalAssertNumRows) {
            AssertNumRowsElement assertNumRowsElement = ((LogicalAssertNumRows) plan).getAssertNumRowsElement();
            if (assertNumRowsElement.getAssertion() == AssertNumRowsElement.Assertion.EQ
                    || assertNumRowsElement.getDesiredNumOfRows() == 1L) {
                return true;
            }
        }
        return false;
    }

    private boolean joinOnAssertOneRowEq(LogicalJoin join) {
        return isAssertOneRowEqOrProjectAssertOneRowEq(join.right())
                || isAssertOneRowEqOrProjectAssertOneRowEq(join.left());
    }

    private Plan pushDownAssertNumRowsJoin(LogicalJoin topJoin) {
        Plan assertBranch = topJoin.right();
        Expression condition = (Expression) topJoin.getOtherJoinConjuncts().get(0);
        List<Alias> aliasUsedInConditionFromLeftProject = new ArrayList<>();
        LogicalJoin<? extends Plan, ? extends Plan> bottomJoin;
        if (topJoin.left() instanceof LogicalProject) {
            LogicalProject<? extends Plan> leftProject = (LogicalProject<? extends Plan>) topJoin.left();
            for (NamedExpression namedExpression : leftProject.getProjects()) {
                if (namedExpression instanceof Alias && condition.getInputSlots().contains(namedExpression.toSlot())) {
                    aliasUsedInConditionFromLeftProject.add((Alias) namedExpression);
                }
            }
            condition = leftProject.pushDownExpressionPastProject(condition);
            bottomJoin = (LogicalJoin<? extends Plan, ? extends Plan>) leftProject.child();
        } else {
            bottomJoin = (LogicalJoin<? extends Plan, ? extends Plan>) topJoin.left();
        }
        Plan bottomLeft = bottomJoin.left();
        Plan bottomRight = bottomJoin.right();

        List<Slot> conditionSlotsFromTopLeft = condition.getInputSlots().stream()
                .filter(slot -> topJoin.left().getOutputSet().contains(slot))
                .collect(Collectors.toList());
        if (bottomLeft.getOutputSet().containsAll(conditionSlotsFromTopLeft)) {
            // push to bottomLeft
            Plan newBottomLeft;
            if (aliasUsedInConditionFromLeftProject.isEmpty()) {
                newBottomLeft = bottomLeft;
            } else {
                newBottomLeft = projectAliasOnPlan(aliasUsedInConditionFromLeftProject, bottomLeft);
            }
            LogicalJoin<? extends Plan, ? extends Plan> newBottomJoin = new LogicalJoin<>(
                    topJoin.getJoinType(),
                    topJoin.getHashJoinConjuncts(),
                    topJoin.getOtherJoinConjuncts(),
                    newBottomLeft,
                    assertBranch,
                    topJoin.getJoinReorderContext());
            LogicalJoin<? extends Plan, ? extends Plan> newTopJoin = (LogicalJoin<? extends Plan, ? extends Plan>)
                    bottomJoin.withChildren(newBottomJoin, bottomRight);
            if (topJoin.left() instanceof LogicalProject) {
                LogicalProject<? extends Plan> upperProject = projectAliasOnPlan(
                        aliasUsedInConditionFromLeftProject, topJoin.left());
                return upperProject.withChildren(newTopJoin);
            } else {
                return newTopJoin;
            }
        } else if (bottomRight.getOutputSet().containsAll(conditionSlotsFromTopLeft)) {
            Plan newBottomRight;
            if (aliasUsedInConditionFromLeftProject.isEmpty()) {
                newBottomRight = bottomRight;
            } else {
                newBottomRight = projectAliasOnPlan(aliasUsedInConditionFromLeftProject, bottomRight);
            }
            LogicalJoin<? extends Plan, ? extends Plan> newBottomJoin = new LogicalJoin<>(
                    topJoin.getJoinType(),
                    topJoin.getHashJoinConjuncts(),
                    topJoin.getOtherJoinConjuncts(),
                    newBottomRight,
                    assertBranch,
                    topJoin.getJoinReorderContext());
            LogicalJoin<? extends Plan, ? extends Plan> newTopJoin = (LogicalJoin<? extends Plan, ? extends Plan>)
                    bottomJoin.withChildren(bottomLeft, newBottomJoin);
            if (topJoin.left() instanceof LogicalProject) {
                LogicalProject<? extends Plan> upperProject = projectAliasOnPlan(
                        aliasUsedInConditionFromLeftProject, topJoin.left());
                return upperProject.withChildren(newTopJoin);
            } else {
                return newTopJoin;
            }
        }
        return null;
    }

    @VisibleForTesting
    LogicalProject<? extends Plan> projectAliasOnPlan(List<Alias> projections, Plan child) {
        if (child instanceof LogicalProject) {
            LogicalProject<? extends Plan> project = (LogicalProject<? extends Plan>) child;
            List<NamedExpression> newProjections =
                    Lists.newArrayList(project.getProjects());
            for (Alias alias : projections) {
                if (!project.getOutput().contains(alias.toSlot())) {
                    NamedExpression expr = (NamedExpression) project.pushDownExpressionPastProject(alias);
                    newProjections.add(expr);
                }
            }
            return project.withProjects(newProjections);
        } else {
            List<NamedExpression> newProjections = Lists.newArrayList(child.getOutput());
            newProjections.addAll(projections);
            return new LogicalProject<>(newProjections, child);
        }
    }
}
