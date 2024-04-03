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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Push down TopN-Distinct through Outer Join into left child .....
 */
public class PushDownTopNDistinctThroughJoin implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // topN -> join
                logicalTopN(logicalAggregate(logicalJoin()).when(LogicalAggregate::isDistinct))
                        // TODO: complex order by
                        .when(topN -> topN.getOrderKeys().stream().map(OrderKey::getExpr)
                                .allMatch(Slot.class::isInstance))
                        .then(topN -> {
                            LogicalAggregate<LogicalJoin<Plan, Plan>> distinct = topN.child();
                            LogicalJoin<Plan, Plan> join = distinct.child();
                            Plan newJoin = pushTopNThroughJoin(topN, join);
                            if (newJoin == null || join.children().equals(newJoin.children())) {
                                return null;
                            }
                            return topN.withChildren(distinct.withChildren(newJoin));
                        })
                        .toRule(RuleType.PUSH_DOWN_TOP_N_DISTINCT_THROUGH_JOIN),

                // topN -> project -> join
                logicalTopN(logicalAggregate(logicalProject(logicalJoin()).when(LogicalProject::isAllSlots))
                        .when(LogicalAggregate::isDistinct))
                        .when(topN -> topN.getOrderKeys().stream().map(OrderKey::getExpr)
                                .allMatch(Slot.class::isInstance))
                        .then(topN -> {
                            LogicalAggregate<LogicalProject<LogicalJoin<Plan, Plan>>> distinct = topN.child();
                            LogicalProject<LogicalJoin<Plan, Plan>> project = distinct.child();
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

                            Plan newJoin = pushTopNThroughJoin(topN, join);
                            if (newJoin == null || join.children().equals(newJoin.children())) {
                                return null;
                            }
                            return topN.withChildren(project.withChildren(distinct.withChildren(newJoin)));
                        }).toRule(RuleType.PUSH_DOWN_TOP_N_DISTINCT_THROUGH_PROJECT_JOIN)
        );
    }

    private Plan pushTopNThroughJoin(LogicalTopN<? extends Plan> topN, LogicalJoin<Plan, Plan> join) {
        Set<Slot> groupBySlots = ((LogicalAggregate<?>) topN.child()).getGroupByExpressions().stream()
                .flatMap(e -> e.getInputSlots().stream()).collect(Collectors.toSet());
        switch (join.getJoinType()) {
            case LEFT_OUTER_JOIN: {
                List<OrderKey> pushedOrderKeys = getPushedOrderKeys(groupBySlots,
                        join.left().getOutputSet(), topN.getOrderKeys());
                if (!pushedOrderKeys.isEmpty()) {
                    LogicalTopN<Plan> left = topN.withLimitOrderKeyAndChild(
                            topN.getLimit() + topN.getOffset(), 0, pushedOrderKeys,
                            PlanUtils.distinct(join.left()));
                    return join.withChildren(left, join.right());
                }
                return null;
            }
            case RIGHT_OUTER_JOIN: {
                List<OrderKey> pushedOrderKeys = getPushedOrderKeys(groupBySlots,
                        join.right().getOutputSet(), topN.getOrderKeys());
                if (!pushedOrderKeys.isEmpty()) {
                    LogicalTopN<Plan> right = topN.withLimitOrderKeyAndChild(
                            topN.getLimit() + topN.getOffset(), 0, pushedOrderKeys,
                            PlanUtils.distinct(join.right()));
                    return join.withChildren(join.left(), right);
                }
                return null;
            }
            case CROSS_JOIN: {
                Plan leftChild = join.left();
                Plan rightChild = join.right();
                List<OrderKey> leftPushedOrderKeys = getPushedOrderKeys(groupBySlots,
                        join.left().getOutputSet(), topN.getOrderKeys());
                if (!leftPushedOrderKeys.isEmpty()) {
                    leftChild = topN.withLimitOrderKeyAndChild(
                            topN.getLimit() + topN.getOffset(), 0, leftPushedOrderKeys,
                            PlanUtils.distinct(join.left()));
                }
                List<OrderKey> rightPushedOrderKeys = getPushedOrderKeys(groupBySlots,
                        join.right().getOutputSet(), topN.getOrderKeys());
                if (!rightPushedOrderKeys.isEmpty()) {
                    rightChild = topN.withLimitOrderKeyAndChild(
                            topN.getLimit() + topN.getOffset(), 0, rightPushedOrderKeys,
                            PlanUtils.distinct(join.right()));
                }
                if (leftChild == join.left() && rightChild == join.right()) {
                    return null;
                } else {
                    return join.withChildren(leftChild, rightChild);
                }
            }
            default:
                // don't push limit.
                return null;
        }
    }

    /**
     * return pushed order-keys. If top-n distinct cannot be pushed, return empty list.
     */
    private List<OrderKey> getPushedOrderKeys(Set<Slot> groupBySlots, Set<Slot> joinChildSlot,
            List<OrderKey> orderKeys) {
        // NOTICE: Currently, we have implemented strict restrictions to ensure that the distinct columns is
        //   a superset of the output from the corresponding child of the join operator. In the future, we can relax
        //   this restriction and only require that there is overlap between the output of the corresponding child of
        //   the join operator and the distinct columns.
        //   However, this would require changes to the optimized plan, converting the pushed-down aggregation distinct
        //   to the window function "row number". Partition by distinct columns, and a filtering condition of
        //   "row number = 1" would be added.
        if (!groupBySlots.containsAll(joinChildSlot)) {
            return ImmutableList.of();
        }
        // we must check the order of order keys. the slot of non-join-output should not appear before join's output
        // other-wise, we will get wrong result, if we push top-n under join.
        ImmutableList.Builder<OrderKey> pushedOrderKeys = ImmutableList.builder();
        boolean notFound = false;
        for (OrderKey orderKey : orderKeys) {
            if (joinChildSlot.contains(orderKey.getExpr())) {
                if (notFound) {
                    return ImmutableList.of();
                } else {
                    pushedOrderKeys.add(orderKey);
                }
            } else {
                notFound = true;
            }
        }
        return pushedOrderKeys.build();
    }
}
