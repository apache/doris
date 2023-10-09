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
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Push down TopN through Outer Join into left child .....
 */
public class PushdownTopNThroughJoin implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // topN -> join
                logicalTopN(logicalJoin())
                        // TODO: complex orderby
                        .when(topN -> topN.getOrderKeys().stream().map(OrderKey::getExpr)
                                .allMatch(Slot.class::isInstance))
                        .then(topN -> {
                            LogicalJoin<Plan, Plan> join = topN.child();
                            Plan newJoin = pushLimitThroughJoin(topN, join);
                            if (newJoin == null || topN.child().children().equals(newJoin.children())) {
                                return null;
                            }
                            return topN.withChildren(newJoin);
                        })
                        .toRule(RuleType.PUSH_TOP_N_THROUGH_JOIN),

                // topN -> project -> join
                logicalTopN(logicalProject(logicalJoin()).when(LogicalProject::isAllSlots))
                        // TODO: complex project
                        .when(topN -> topN.getOrderKeys().stream().map(OrderKey::getExpr)
                                .allMatch(Slot.class::isInstance))
                        .then(topN -> {
                            LogicalProject<LogicalJoin<Plan, Plan>> project = topN.child();
                            LogicalJoin<Plan, Plan> join = project.child();

                            Plan newJoin = pushLimitThroughJoin(topN, join);
                            if (newJoin == null || join.children().equals(newJoin.children())) {
                                return null;
                            }
                            return topN.withChildren(project.withChildren(newJoin));
                        }).toRule(RuleType.PUSH_TOP_N_THROUGH_PROJECT_JOIN)
        );
    }

    private Plan pushLimitThroughJoin(LogicalTopN<? extends Plan> topN, LogicalJoin<Plan, Plan> join) {
        List<Slot> orderbySlots = topN.getOrderKeys().stream().map(OrderKey::getExpr)
                .flatMap(e -> e.getInputSlots().stream()).collect(Collectors.toList());
        switch (join.getJoinType()) {
            case LEFT_OUTER_JOIN:
                if (join.left().getOutputSet().containsAll(orderbySlots)) {
                    return join.withChildren(
                            topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0, join.left()),
                            join.right());
                }
                return null;
            case RIGHT_OUTER_JOIN:
                if (join.right().getOutputSet().containsAll(orderbySlots)) {
                    return join.withChildren(
                            join.left(),
                            topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0, join.right()));
                }
                return null;
            case CROSS_JOIN:

                if (join.left().getOutputSet().containsAll(orderbySlots)) {
                    return join.withChildren(
                            topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0, join.left()),
                            join.right());
                } else if (join.right().getOutputSet().containsAll(orderbySlots)) {
                    return join.withChildren(
                            join.left(),
                            topN.withLimitChild(topN.getLimit() + topN.getOffset(), 0, join.right()));
                } else {
                    return null;
                }
            default:
                // don't push limit.
                return null;
        }
    }
}
