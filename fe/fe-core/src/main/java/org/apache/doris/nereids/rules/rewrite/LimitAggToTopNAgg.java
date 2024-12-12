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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * convert limit->agg to topn->agg
 * if all group keys are in limit.output
 * to enable
 * 1. topn-filter
 * 2. push limit to local agg
 */
public class LimitAggToTopNAgg implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // limit -> agg to topn->agg
                logicalLimit(logicalAggregate())
                        .when(limit -> ConnectContext.get() != null
                                && ConnectContext.get().getSessionVariable().pushTopnToAgg
                                && ConnectContext.get().getSessionVariable().topnOptLimitThreshold
                                >= limit.getLimit() + limit.getOffset())
                        .then(limit -> {
                            LogicalAggregate<? extends Plan> agg = limit.child();
                            List<OrderKey> orderKeys = generateOrderKeyByGroupKey(agg);
                            return new LogicalTopN<>(orderKeys, limit.getLimit(), limit.getOffset(), agg);
                        }).toRule(RuleType.LIMIT_AGG_TO_TOPN_AGG),
                //limit->project->agg to project->topn->agg
                logicalLimit(logicalProject(logicalAggregate()))
                        .when(limit -> ConnectContext.get() != null
                                && ConnectContext.get().getSessionVariable().pushTopnToAgg
                                && ConnectContext.get().getSessionVariable().topnOptLimitThreshold
                                >= limit.getLimit() + limit.getOffset())
                        .when(limit -> limit.child().isAllSlots())
                        .then(limit -> {
                            LogicalProject<? extends Plan> project = limit.child();
                            LogicalAggregate<? extends Plan> agg
                                    = (LogicalAggregate<? extends Plan>) project.child();
                            List<OrderKey> orderKeys = generateOrderKeyByGroupKey(agg);
                            LogicalTopN topn = new LogicalTopN<>(orderKeys, limit.getLimit(),
                                    limit.getOffset(), agg);
                            project = (LogicalProject<? extends Plan>) project.withChildren(topn);
                            return project;
                        }).toRule(RuleType.LIMIT_AGG_TO_TOPN_AGG),
                // topn -> agg: append group key(if it is not sort key) to sort key
                logicalTopN(logicalAggregate())
                        .when(topn -> ConnectContext.get() != null
                                && ConnectContext.get().getSessionVariable().pushTopnToAgg
                                && ConnectContext.get().getSessionVariable().topnOptLimitThreshold
                                >= topn.getLimit() + topn.getOffset())
                        .then(topn -> {
                            LogicalAggregate<? extends Plan> agg = (LogicalAggregate<? extends Plan>) topn.child();
                            List<OrderKey> newOrders = Lists.newArrayList(topn.getOrderKeys());
                            Set<Expression> orderExprs = topn.getOrderKeys().stream()
                                    .map(orderKey -> orderKey.getExpr()).collect(Collectors.toSet());
                            boolean orderKeyChanged = false;
                            for (Expression expr : agg.getGroupByExpressions()) {
                                if (!orderExprs.contains(expr)) {
                                    // after NormalizeAggregate, expr should be SlotReference
                                    if (expr instanceof SlotReference) {
                                        orderKeyChanged = true;
                                        newOrders.add(new OrderKey(expr, true, true));
                                    }
                                }
                            }
                            return orderKeyChanged ? topn.withOrderKeys(newOrders) : topn;
                        }).toRule(RuleType.LIMIT_AGG_TO_TOPN_AGG),
                //topn -> project ->agg: add all group key to sort key, and prune column
                logicalTopN(logicalProject(logicalAggregate()))
                        .when(topn -> ConnectContext.get() != null
                                && ConnectContext.get().getSessionVariable().pushTopnToAgg
                                && ConnectContext.get().getSessionVariable().topnOptLimitThreshold
                                >= topn.getLimit() + topn.getOffset())
                        .when(topn -> topn.child().isAllSlots())
                        .then(topn -> {
                            LogicalProject project = topn.child();
                            LogicalAggregate<? extends Plan> agg = (LogicalAggregate) project.child();
                            List<OrderKey> newOrders = Lists.newArrayList(topn.getOrderKeys());
                            Set<Expression> orderExprs = topn.getOrderKeys().stream()
                                    .map(orderKey -> orderKey.getExpr()).collect(Collectors.toSet());
                            boolean orderKeyChanged = false;
                            for (Expression expr : agg.getGroupByExpressions()) {
                                if (!orderExprs.contains(expr)) {
                                    // after NormalizeAggregate, expr should be SlotReference
                                    if (expr instanceof SlotReference) {
                                        orderKeyChanged = true;
                                        newOrders.add(new OrderKey(expr, true, true));
                                    }
                                }
                            }
                            Plan result;
                            if (orderKeyChanged) {
                                topn = (LogicalTopN) topn.withChildren(agg);
                                topn.withOrderKeys(newOrders);
                                result = (Plan) project.withChildren(topn);
                            } else {
                                result = topn;
                            }
                            return result;
                        }).toRule(RuleType.LIMIT_AGG_TO_TOPN_AGG)
        );
    }

    private List<OrderKey> generateOrderKeyByGroupKey(LogicalAggregate<? extends Plan> agg) {
        return agg.getGroupByExpressions().stream()
            .map(key -> new OrderKey(key, true, false))
            .collect(Collectors.toList());
    }
}
