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
                logicalTopN(logicalAggregate(logicalJoin()).when(a -> a.isDistinct()))
                        // TODO: complex orderby
                        .when(topN -> topN.getOrderKeys().stream().map(OrderKey::getExpr)
                                .allMatch(Slot.class::isInstance))
                        .then(topN -> {
                            LogicalAggregate<LogicalJoin<Plan, Plan>> distinct = topN.child();
                            LogicalJoin<Plan, Plan> join = distinct.child();
                            Plan newJoin = pushTopNThroughJoin(topN, join);
                            if (newJoin == null || topN.child().children().equals(newJoin.children())) {
                                return null;
                            }
                            return topN.withChildren(distinct.withChildren(newJoin));
                        })
                        .toRule(RuleType.PUSH_DOWN_TOP_N_DISTINCT_THROUGH_JOIN),

                // topN -> project -> join
                logicalTopN(logicalAggregate(logicalProject(logicalJoin()).when(p -> p.isAllSlots()))
                        .when(a -> a.isDistinct()))
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
        List<Slot> groupBySlots = ((LogicalAggregate<?>) topN.child()).getGroupByExpressions().stream()
                .flatMap(e -> e.getInputSlots().stream()).collect(Collectors.toList());
        switch (join.getJoinType()) {
            case LEFT_OUTER_JOIN:
                if (join.left().getOutputSet().containsAll(groupBySlots)) {
                    LogicalTopN<Plan> left = topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0,
                            PlanUtils.distinct(join.left()));
                    return join.withChildren(left, join.right());
                }
                return null;
            case RIGHT_OUTER_JOIN:
                if (join.right().getOutputSet().containsAll(groupBySlots)) {
                    LogicalTopN<Plan> right = topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0,
                            PlanUtils.distinct(join.right()));
                    return join.withChildren(join.left(), right);
                }
                return null;
            case CROSS_JOIN:
                if (join.left().getOutputSet().containsAll(groupBySlots)) {
                    LogicalTopN<Plan> left = topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0,
                            PlanUtils.distinct(join.left()));
                    return join.withChildren(left, join.right());
                } else if (join.right().getOutputSet().containsAll(groupBySlots)) {
                    LogicalTopN<Plan> right = topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0,
                            PlanUtils.distinct(join.right()));
                    return join.withChildren(join.left(), right);
                } else {
                    return null;
                }
            default:
                // don't push limit.
                return null;
        }
    }
}
