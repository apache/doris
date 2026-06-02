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
                    if (!canPushToChild(join.left(), requiredOutputSlots)) {
                        return null;
                    }
                    return join.withChildren(
                            pushTopNToChild(topN, join.left(), orderbySlots, requiredOutputSlots),
                            join.right());
                }
                return null;
            case RIGHT_OUTER_JOIN:
            case ASOF_RIGHT_OUTER_JOIN:
                if (join.right() instanceof TopN) {
                    return null;
                }
                if (join.right().getOutputSet().containsAll(orderbySlots)) {
                    if (!canPushToChild(join.right(), requiredOutputSlots)) {
                        return null;
                    }
                    return join.withChildren(
                            join.left(),
                            pushTopNToChild(topN, join.right(), orderbySlots, requiredOutputSlots));
                }
                return null;
            case CROSS_JOIN:
                if (join.left().getOutputSet().containsAll(orderbySlots)) {
                    if (join.left() instanceof TopN) {
                        return null;
                    }
                    if (!canPushToChild(join.left(), requiredOutputSlots)) {
                        return null;
                    }
                    return join.withChildren(
                            pushTopNToChild(topN, join.left(), orderbySlots, requiredOutputSlots),
                            join.right());
                } else if (join.right().getOutputSet().containsAll(orderbySlots)) {
                    if (join.right() instanceof TopN) {
                        return null;
                    }
                    if (!canPushToChild(join.right(), requiredOutputSlots)) {
                        return null;
                    }
                    return join.withChildren(
                            join.left(),
                            pushTopNToChild(topN, join.right(), orderbySlots, requiredOutputSlots));
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

    private Plan pushTopNToChild(LogicalTopN<? extends Plan> topN, Plan child, List<Slot> orderbySlots,
            List<Slot> requiredOutputSlots) {
        if (requiredOutputSlots.isEmpty()) {
            return topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0, child);
        }

        Set<Slot> requiredSet = new HashSet<>(requiredOutputSlots);
        requiredSet.addAll(orderbySlots);
        List<NamedExpression> childOutput = child.getOutput().stream()
                .filter(requiredSet::contains)
                .collect(Collectors.toList());
        Plan topNChild = PlanUtils.projectOrSelf(childOutput, child);
        return topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0, topNChild);
    }
}
