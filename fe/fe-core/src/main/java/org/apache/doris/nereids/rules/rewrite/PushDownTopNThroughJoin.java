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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Push down TopN through Outer Join into left child .....
 */
public class PushDownTopNThroughJoin implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // topN -> join
                logicalTopN(logicalJoin())
                        // TODO: complex orderby
                        .when(topn ->
                                ConnectContext.get() != null
                                        && ConnectContext.get().getSessionVariable().topnOptLimitThreshold
                                        >= topn.getLimit() + topn.getOffset())
                        .when(topN -> topN.getOrderKeys().stream().map(OrderKey::getExpr)
                                .allMatch(Slot.class::isInstance))
                        .then(topN -> {
                            LogicalJoin<Plan, Plan> join = topN.child();
                            Plan newJoin = pushLimitThroughJoin(topN, join, ImmutableList.of());
                            if (newJoin == null) {
                                return null;
                            }
                            return topN.withChildren(newJoin);
                        })
                        .toRule(RuleType.PUSH_DOWN_TOP_N_THROUGH_JOIN),

                // topN -> project -> join
                logicalTopN(logicalProject(logicalJoin()))
                        .when(topN -> topN.getOrderKeys().stream().map(OrderKey::getExpr)
                                .allMatch(Slot.class::isInstance))
                        .then(topN -> {
                            LogicalProject<LogicalJoin<Plan, Plan>> project = topN.child();
                            LogicalJoin<Plan, Plan> join = project.child();

                            // If orderby exprs aren't all in the output of the project, we can't push down.
                            // topN(order by: slot(a+1))
                            // - project(a+1, b)
                            // TODO: in the future, we also can push down it.
                            Set<Slot> outputSet = project.child().getOutputSet();
                            if (!topN.getOrderKeys().stream().map(OrderKey::getExpr)
                                    .flatMap(e -> e.getInputSlots().stream())
                                    .allMatch(outputSet::contains)) {
                                return null;
                            }

                            List<Slot> projectInputSlots = project.getProjects().stream()
                                    .flatMap(ne -> ne instanceof Slot
                                            ? Stream.of((Slot) ne) : ne.getInputSlots().stream())
                                    .collect(Collectors.toList());
                            if (!join.getOutputSet().containsAll(projectInputSlots)) {
                                return null;
                            }
                            // Verify the push-down target child has all required
                            // slots. Intermediate Projects (e.g. from ColumnPruning)
                            // may restrict output, and push-down would break the plan.
                            List<Slot> orderbySlots = topN.getOrderKeys().stream()
                                    .map(OrderKey::getExpr)
                                    .flatMap(e -> e.getInputSlots().stream())
                                    .collect(Collectors.toList());
                            Plan targetChild = join.left().getOutputSet().containsAll(orderbySlots)
                                    ? join.left() : join.right();
                            if (!targetChild.getOutputSet().containsAll(projectInputSlots)) {
                                return null;
                            }
                            Plan newJoin = pushLimitThroughJoin(topN, join, projectInputSlots);
                            if (newJoin == null) {
                                return null;
                            }
                            return topN.withChildren(project.withChildren(newJoin));
                        }).toRule(RuleType.PUSH_DOWN_TOP_N_THROUGH_PROJECT_JOIN)
        );
    }

    private Plan pushLimitThroughJoin(LogicalTopN<? extends Plan> topN, LogicalJoin<Plan, Plan> join,
            List<Slot> requiredOutputSlots) {
        List<Slot> orderbySlots = topN.getOrderKeys().stream().map(OrderKey::getExpr)
                .flatMap(e -> e.getInputSlots().stream()).collect(Collectors.toList());
        switch (join.getJoinType()) {
            case LEFT_OUTER_JOIN:
            case ASOF_LEFT_OUTER_JOIN:
                if (join.left() instanceof TopN) {
                    return null;
                }
                if (join.left().getOutputSet().containsAll(orderbySlots)) {
                    List<Slot> childRequiredOutputSlots = buildRequiredOutputSlots(
                            requiredOutputSlots, join.getLeftConditionSlot());
                    return join.withChildren(
                            pushTopNToChild(topN, join.left(), orderbySlots, childRequiredOutputSlots),
                            join.right());
                }
                return null;
            case RIGHT_OUTER_JOIN:
            case ASOF_RIGHT_OUTER_JOIN:
                if (join.right() instanceof TopN) {
                    return null;
                }
                if (join.right().getOutputSet().containsAll(orderbySlots)) {
                    List<Slot> childRequiredOutputSlots = buildRequiredOutputSlots(
                            requiredOutputSlots, join.getRightConditionSlot());
                    return join.withChildren(
                            join.left(),
                            pushTopNToChild(topN, join.right(), orderbySlots, childRequiredOutputSlots));
                }
                return null;
            case CROSS_JOIN:
                if (join.left().getOutputSet().containsAll(orderbySlots)) {
                    if (join.left() instanceof TopN) {
                        return null;
                    }
                    List<Slot> childRequiredOutputSlots = buildRequiredOutputSlots(
                            requiredOutputSlots, join.getLeftConditionSlot());
                    return join.withChildren(
                            pushTopNToChild(topN, join.left(), orderbySlots, childRequiredOutputSlots),
                            join.right());
                } else if (join.right().getOutputSet().containsAll(orderbySlots)) {
                    if (join.right() instanceof TopN) {
                        return null;
                    }
                    List<Slot> childRequiredOutputSlots = buildRequiredOutputSlots(
                            requiredOutputSlots, join.getRightConditionSlot());
                    return join.withChildren(
                            join.left(),
                            pushTopNToChild(topN, join.right(), orderbySlots, childRequiredOutputSlots));
                } else {
                    return null;
                }
            default:
                // don't push limit.
                return null;
        }
    }

    private boolean canPushToChild(Plan child, List<Slot> requiredOutputSlots) {
        return requiredOutputSlots.isEmpty() || child.getOutputSet().containsAll(requiredOutputSlots);
    }

    private List<Slot> buildRequiredOutputSlots(List<Slot> requiredOutputSlots, Set<Slot> conditionSlots) {
        Set<Slot> requiredSet = new HashSet<>(requiredOutputSlots);
        requiredSet.addAll(conditionSlots);
        return ImmutableList.copyOf(requiredSet);
    }

    private Plan pushTopNToChild(LogicalTopN<? extends Plan> topN, Plan child, List<Slot> orderbySlots,
            List<Slot> requiredOutputSlots) {
        // Keep only the slots that this child actually outputs. When pushing to
        // one side of an outer join, requiredOutputSlots may include columns from
        // the other side which this child cannot provide.
        Set<Slot> childOutputSet = child.getOutputSet();
        List<Slot> childRequired = requiredOutputSlots.stream()
                .filter(childOutputSet::contains)
                .collect(Collectors.toList());
        if (childRequired.isEmpty()) {
            return topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0, child);
        }
        // Ensure the child outputs all required columns. If the child already
        // outputs everything needed, no extra Project is required.
        Set<Slot> requiredSet = new HashSet<>(childRequired);
        requiredSet.addAll(orderbySlots);
        if (childOutputSet.containsAll(requiredSet)) {
            return topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0, child);
        }
        List<NamedExpression> childOutput = child.getOutput().stream()
                .filter(requiredSet::contains)
                .collect(Collectors.toList());
        Plan topNChild = PlanUtils.projectOrSelf(childOutput, child);
        return topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0, topNChild);
    }
}
