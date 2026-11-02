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

package org.apache.doris.nereids.rules.exploration;

import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.Utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Common
 */
public class CBOUtils {
    public static Set<Slot> joinChildConditionSlots(LogicalJoin<? extends Plan, ? extends Plan> join, boolean left) {
        Set<Slot> childSlots = left ? join.left().getOutputSet() : join.right().getOutputSet();
        return join.getConditionSlot().stream()
                .filter(childSlots::contains)
                .collect(Collectors.toSet());
    }

    /**
     * Collect the exprIds of the project items which read no input, e.g. the constant materialized by constant
     * propagation, now() or 1 + 1.
     *
     * <p>Such an item can not be pushed into a child of a join: the join condition may refer to it, and pushing the
     * item into one side while the condition keeps referring to it from the other side makes both children of the
     * join output the same exprId. PhysicalHashJoin#getHashConjunctsExprIds splits the join keys by exprId only, so
     * both keys would be taken as the keys of one side and the required hash properties of the other side stay
     * empty.
     */
    public static Set<ExprId> collectNoInputProjectItemIds(List<NamedExpression> projectItems) {
        Set<ExprId> noInputProjectItemIds = new HashSet<>();
        for (NamedExpression item : projectItems) {
            if (item.getInputSlotExprIds().isEmpty()) {
                noInputProjectItemIds.add(item.getExprId());
            }
        }
        return noInputProjectItemIds;
    }

    /** whether the join condition refers to any of the given exprIds */
    public static boolean joinConditionRefersTo(
            LogicalJoin<? extends Plan, ? extends Plan> join, Set<ExprId> exprIds) {
        if (exprIds.isEmpty()) {
            return false;
        }
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            if (refersToAny(conjunct, exprIds)) {
                return true;
            }
        }
        for (Expression conjunct : join.getOtherJoinConjuncts()) {
            if (refersToAny(conjunct, exprIds)) {
                return true;
            }
        }
        return false;
    }

    private static boolean refersToAny(Expression expression, Set<ExprId> exprIds) {
        for (Slot slot : expression.getInputSlots()) {
            if (exprIds.contains(slot.getExprId())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The project above a join after pushing its items down: an item which reads no input stays in this project and
     * keeps its expression, an item which has been pushed down is referenced by its output slot of the join below.
     */
    public static List<NamedExpression> projectAboveJoin(
            LogicalProject<?> project, Set<ExprId> noInputProjectItemIds) {
        List<NamedExpression> projectItems = new ArrayList<>(project.getProjects().size());
        for (NamedExpression item : project.getProjects()) {
            projectItems.add(noInputProjectItemIds.contains(item.getExprId()) ? item : item.toSlot());
        }
        return projectItems;
    }

    public static Plan newProjectIfNeeded(Set<ExprId> requiredExprIds, Plan plan) {
        if (requiredExprIds.equals(plan.getOutputExprIdSet())) {
            return plan;
        }
        return newProject(requiredExprIds, plan);
    }

    public static Plan newProject(Set<ExprId> requiredExprIds, Plan plan) {
        List<NamedExpression> projects = plan.getOutput().stream()
                .filter(namedExpr -> requiredExprIds.contains(namedExpr.getExprId()))
                .collect(Collectors.toList());
        return new LogicalProject<>(projects, plan);
    }

    /**
     * Split topJoin Condition to two part according to include bExprIdSet.
     */
    public static Map<Boolean, List<Expression>> splitConjuncts(List<Expression> topConjuncts,
            List<Expression> bottomConjuncts, Set<ExprId> bExprIdSet) {
        // top: (A B)(error) (A C) (B C) (A B C)
        // Split topJoin Condition to two part according to include B.
        Map<Boolean, List<Expression>> splitOn = topConjuncts.stream()
                .collect(Collectors.partitioningBy(topHashOn -> {
                    Set<ExprId> usedExprIds = topHashOn.getInputSlotExprIds();
                    return Utils.isIntersecting(usedExprIds, bExprIdSet);
                }));
        // * don't include B, just include (A C)
        // we add it into newBottomJoin HashConjuncts.
        // * include B, include (A B C) or (A B)
        // we add it into newTopJoin HashConjuncts.
        List<Expression> newTopHashConjuncts = splitOn.get(true);
        newTopHashConjuncts.addAll(bottomConjuncts);

        return splitOn;
    }
}
