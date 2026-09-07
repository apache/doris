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

import org.apache.doris.nereids.rules.exploration.CBOUtils;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Normalize a Project over Join only when a Project-aware join reorder rule produces an alternative. */
final class ProjectJoinReorderHelper {
    private ProjectJoinReorderHelper() {
    }

    /**
     * Keep a slot-only Project unchanged, or push single-side complex expressions below its Join.
     *
     * <p>The returned Project is slot-only and has the same output as the input Project. Empty means that
     * the Project cannot be moved without changing semantics.</p>
     */
    static Optional<LogicalProject<LogicalJoin<Plan, Plan>>> normalize(LogicalProject<?> project) {
        if (project.isAllSlots()) {
            return Optional.of(castProject(project));
        }

        LogicalJoin<Plan, Plan> join = childJoin(project);
        JoinType joinType = join.getJoinType();
        if (joinType.isLeftSemiOrAntiJoin()) {
            if (join.isMarkJoin() || projectBothJoinSides(project)) {
                return Optional.empty();
            }
            return Optional.of(pushDownLeftSemiProject(project, join));
        }
        if (joinType.isInnerJoin() || joinType.isOuterJoin()
                || joinType.isAsofInnerJoin() || joinType.isAsofOuterJoin()) {
            return pushDownInnerOuterProject(project, join);
        }
        return Optional.empty();
    }

    private static boolean projectBothJoinSides(LogicalProject<?> project) {
        LogicalJoin<Plan, Plan> join = childJoin(project);
        Set<Slot> projectOutput = project.getOutputSet();
        boolean containLeft = join.left().getOutput().stream().anyMatch(projectOutput::contains);
        boolean containRight = join.right().getOutput().stream().anyMatch(projectOutput::contains);
        return containLeft && containRight;
    }

    private static LogicalProject<LogicalJoin<Plan, Plan>> pushDownLeftSemiProject(
            LogicalProject<?> project, LogicalJoin<Plan, Plan> join) {
        Set<Slot> conditionLeftSlots = CBOUtils.joinChildConditionSlots(join, true);
        List<NamedExpression> newProjects = new ArrayList<>(project.getProjects());
        Set<Slot> projectUsedSlots = project.getProjects().stream()
                .map(NamedExpression::toSlot)
                .collect(Collectors.toSet());
        conditionLeftSlots.stream()
                .filter(slot -> !projectUsedSlots.contains(slot))
                .forEach(newProjects::add);

        Plan newLeft = new LogicalProject<>(newProjects, join.left());
        LogicalJoin<Plan, Plan> newJoin = join.withChildren(ImmutableList.of(newLeft, join.right()));
        return new LogicalProject<>(ImmutableList.copyOf(project.getOutput()), newJoin);
    }

    private static Optional<LogicalProject<LogicalJoin<Plan, Plan>>> pushDownInnerOuterProject(
            LogicalProject<?> project, LogicalJoin<Plan, Plan> join) {
        Set<ExprId> leftOutputExprIds = join.left().getOutputExprIdSet();
        Set<ExprId> rightOutputExprIds = join.right().getOutputExprIdSet();

        boolean containsHyperEdge = project.getProjects().stream().anyMatch(expression -> {
            Set<ExprId> inputExprIds = expression.getInputSlotExprIds();
            return !leftOutputExprIds.containsAll(inputExprIds)
                    && !rightOutputExprIds.containsAll(inputExprIds);
        });
        if (containsHyperEdge) {
            return Optional.empty();
        }

        List<NamedExpression> projects = adjustProjectsNullable(project, join);
        List<NamedExpression> leftProjects = new ArrayList<>();
        List<NamedExpression> rightProjects = new ArrayList<>();
        for (NamedExpression expression : projects) {
            if (leftOutputExprIds.containsAll(expression.getInputSlotExprIds())) {
                leftProjects.add(expression);
            } else {
                rightProjects.add(expression);
            }
        }

        boolean leftContainsComplexExpression = leftProjects.stream()
                .anyMatch(expression -> !(expression instanceof Slot));
        boolean rightContainsComplexExpression = rightProjects.stream()
                .anyMatch(expression -> !(expression instanceof Slot));
        // JoinCommute supplies the orientation in which a movable complex expression is on the left.
        if (!leftContainsComplexExpression) {
            return Optional.empty();
        }
        if ((join.getJoinType().isRightSideNullable() && rightContainsComplexExpression)
                || (join.getJoinType().isLeftSideNullable() && leftContainsComplexExpression)) {
            return Optional.empty();
        }

        Builder<NamedExpression> newLeftProjects = ImmutableList.<NamedExpression>builder()
                .addAll(leftProjects);
        Set<Slot> leftConditionSlots = CBOUtils.joinChildConditionSlots(join, true);
        Set<Slot> leftProjectSlots = leftProjects.stream()
                .map(NamedExpression::toSlot)
                .collect(Collectors.toSet());
        leftConditionSlots.stream()
                .filter(slot -> !leftProjectSlots.contains(slot))
                .forEach(newLeftProjects::add);
        Plan newLeft = new LogicalProject<>(newLeftProjects.build(), join.left());

        Plan newRight = join.right();
        if (rightContainsComplexExpression) {
            Builder<NamedExpression> newRightProjects = ImmutableList.<NamedExpression>builder()
                    .addAll(rightProjects);
            Set<Slot> rightConditionSlots = CBOUtils.joinChildConditionSlots(join, false);
            Set<Slot> rightProjectSlots = rightProjects.stream()
                    .map(NamedExpression::toSlot)
                    .collect(Collectors.toSet());
            rightConditionSlots.stream()
                    .filter(slot -> !rightProjectSlots.contains(slot))
                    .forEach(newRightProjects::add);
            newRight = new LogicalProject<>(newRightProjects.build(), join.right());
        }

        LogicalJoin<Plan, Plan> newJoin = join.withChildren(ImmutableList.of(newLeft, newRight));
        return Optional.of(new LogicalProject<>(ImmutableList.copyOf(project.getOutput()), newJoin));
    }

    private static List<NamedExpression> adjustProjectsNullable(
            LogicalProject<?> project, LogicalJoin<Plan, Plan> join) {
        if (join.getJoinType().isInnerJoin() || join.getJoinType().isAsofInnerJoin()) {
            return project.getProjects();
        }

        Map<Slot, Slot> childSlots = new HashMap<>();
        join.left().getOutputSet().forEach(slot -> childSlots.put(slot, slot));
        join.right().getOutputSet().forEach(slot -> childSlots.put(slot, slot));
        join.getOutputSet().forEach(slot -> {
            if (childSlots.containsKey(slot)) {
                childSlots.put(slot, childSlots.get(slot));
            }
        });
        return project.getProjects().stream()
                .map(expression -> expression.rewriteUp(child ->
                        child instanceof Slot ? childSlots.get((Slot) child) : child))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private static LogicalJoin<Plan, Plan> childJoin(LogicalProject<?> project) {
        return (LogicalJoin<Plan, Plan>) project.child();
    }

    @SuppressWarnings("unchecked")
    private static LogicalProject<LogicalJoin<Plan, Plan>> castProject(LogicalProject<?> project) {
        return (LogicalProject<LogicalJoin<Plan, Plan>>) project;
    }
}
