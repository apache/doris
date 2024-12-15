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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
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
                        .when(limit -> {
                            LogicalAggregate<? extends Plan> agg = limit.child();
                            return isSortableAggregate(agg);
                        })
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
                        .when(limit -> {
                            LogicalAggregate<? extends Plan> agg = limit.child().child();
                            return isSortableAggregate(agg);
                        })
                        .then(limit -> {
                            LogicalProject<? extends Plan> project = limit.child();
                            LogicalAggregate<? extends Plan> agg = (LogicalAggregate<? extends Plan>) project.child();
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
                        .when(topn -> {
                            LogicalAggregate<? extends Plan> agg = topn.child();
                            return isSortableAggregate(agg);
                        })
                        .then(topn -> {
                            LogicalAggregate<? extends Plan> agg = topn.child();
                            List<OrderKey> newOrderKyes = supplementOrderKeyByGroupKeyIfCompatible(topn, agg);
                            if (newOrderKyes.isEmpty()) {
                                return topn;
                            } else {
                                return topn.withOrderKeys(newOrderKyes);
                            }
                        }).toRule(RuleType.LIMIT_AGG_TO_TOPN_AGG),
                //topn -> project ->agg: add all group key to sort key, and prune column
                logicalTopN(logicalProject(logicalAggregate()))
                        .when(topn -> ConnectContext.get() != null
                                && ConnectContext.get().getSessionVariable().pushTopnToAgg
                                && ConnectContext.get().getSessionVariable().topnOptLimitThreshold
                                >= topn.getLimit() + topn.getOffset())
                        .when(topn -> topn.child().isAllSlots())
                        .when(topn -> {
                            LogicalAggregate<? extends Plan> agg = topn.child().child();
                            return isSortableAggregate(agg);
                        })
                        .then(topn -> {
                            LogicalProject<? extends Plan> project = topn.child();
                            LogicalAggregate<? extends Plan> agg = (LogicalAggregate) project.child();
                            List<OrderKey> newOrders = supplementOrderKeyByGroupKeyIfCompatible(topn, agg);
                            if (newOrders.isEmpty()) {
                                return topn;
                            } else {
                                topn = (LogicalTopN) topn.withChildren(agg);
                                topn = (LogicalTopN) topn.withOrderKeys(newOrders);
                                project = (LogicalProject) project.withChildren(topn);
                                return project;
                            }
                        }).toRule(RuleType.LIMIT_AGG_TO_TOPN_AGG)
        );
    }

    /**
     * not scalar agg
     * no distinct
     */
    public static boolean isSortableAggregate(Aggregate agg) {
        return !agg.getGroupByExpressions().isEmpty() && agg.getDistinctArguments().isEmpty();
    }

    private List<OrderKey> generateOrderKeyByGroupKey(LogicalAggregate<? extends Plan> agg) {
        return agg.getGroupByExpressions().stream()
            .map(key -> new OrderKey(key, true, false))
            .collect(Collectors.toList());
    }

    private List<OrderKey> supplementOrderKeyByGroupKeyIfCompatible(LogicalTopN<? extends Plan> topn,
                                                                    LogicalAggregate<? extends Plan> agg) {
        int groupKeyCount = agg.getGroupByExpressions().size();
        int orderKeyCount = topn.getOrderKeys().size();
        if (orderKeyCount <= groupKeyCount) {
            boolean canAppendOrderKey = true;
            for (int i = 0; i < orderKeyCount; i++) {
                Expression groupKey = agg.getGroupByExpressions().get(i);
                Expression orderKey = topn.getOrderKeys().get(i).getExpr();
                if (!groupKey.equals(orderKey)) {
                    canAppendOrderKey = false;
                    break;
                }
            }
            if (canAppendOrderKey && orderKeyCount < groupKeyCount) {
                List<OrderKey> newOrderKeys = Lists.newArrayList(topn.getOrderKeys());
                for (int i = orderKeyCount; i < groupKeyCount; i++) {
                    newOrderKeys.add(new OrderKey(agg.getGroupByExpressions().get(i), true, false));
                }
                return newOrderKeys;
            } else {
                return Lists.newArrayList();
            }
        } else {
            return Lists.newArrayList();
        }
    }
}
