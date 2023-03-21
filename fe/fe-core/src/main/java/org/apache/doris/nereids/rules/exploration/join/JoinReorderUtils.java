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

import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Common
 */
class JoinReorderUtils {
    static boolean isAllSlotProject(LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project) {
        return project.getProjects().stream().allMatch(expr -> expr instanceof Slot);
    }

    static Map<Boolean, List<NamedExpression>> splitProjection(List<NamedExpression> projects, Plan splitChild) {
        Set<ExprId> splitExprIds = splitChild.getOutputExprIdSet();

        return projects.stream()
                .collect(Collectors.partitioningBy(projectExpr -> {
                    Set<ExprId> usedExprIds = projectExpr.getInputSlotExprIds();
                    return splitExprIds.containsAll(usedExprIds);
                }));
    }

    public static Set<ExprId> combineProjectAndChildExprId(Plan b, List<NamedExpression> bProject) {
        return Stream.concat(
                b.getOutput().stream().map(NamedExpression::getExprId),
                bProject.stream().map(NamedExpression::getExprId)).collect(Collectors.toSet());
    }

    /**
     * If projectExprs is empty or project output equal plan output, return the original plan.
     */
    public static Plan projectOrSelf(List<NamedExpression> projectExprs, Plan plan) {
        if (projectExprs.isEmpty() || projectExprs.stream().map(NamedExpression::getExprId).collect(Collectors.toSet())
                .equals(plan.getOutputExprIdSet())) {
            return plan;
        }
        return new LogicalProject<>(projectExprs, plan);
    }

    public static Plan projectOrSelfInOrder(List<NamedExpression> projectExprs, Plan plan) {
        if (projectExprs.isEmpty() || projectExprs.equals(plan.getOutput())) {
            return plan;
        }
        return new LogicalProject<>(projectExprs, plan);
    }

    /**
     * replace JoinConjuncts by using slots map.
     */
    public static List<Expression> replaceJoinConjuncts(List<Expression> joinConjuncts,
            Map<ExprId, Slot> replaceMaps) {
        return joinConjuncts.stream()
                .map(expr ->
                        expr.rewriteUp(e -> {
                            if (e instanceof Slot && replaceMaps.containsKey(((Slot) e).getExprId())) {
                                return replaceMaps.get(((Slot) e).getExprId());
                            } else {
                                return e;
                            }
                        })
                ).collect(ImmutableList.toImmutableList());
    }

    /**
     * When project not empty, we add all slots used by hashOnCondition into projects.
     */
    public static void addSlotsUsedByOn(Set<Slot> usedSlots, List<NamedExpression> projects) {
        if (projects.isEmpty()) {
            return;
        }
        Set<ExprId> projectExprIdSet = projects.stream()
                .map(NamedExpression::getExprId)
                .collect(Collectors.toSet());
        usedSlots.forEach(slot -> {
            if (!projectExprIdSet.contains(slot.getExprId())) {
                projects.add(slot);
            }
        });
    }

    public static Set<Slot> joinChildConditionSlots(LogicalJoin<? extends Plan, ? extends Plan> join, boolean left) {
        Set<Slot> childSlots = left ? join.left().getOutputSet() : join.right().getOutputSet();
        return join.getConditionSlot().stream()
                .filter(childSlots::contains)
                .collect(Collectors.toSet());
    }
}
