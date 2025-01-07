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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
                        .when(limit -> {
                            LogicalAggregate<? extends Plan> agg = limit.child();
                            return !agg.getGroupByExpressions().isEmpty() && !agg.getSourceRepeat().isPresent();
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
                        .when(limit -> {
                            LogicalAggregate<? extends Plan> agg = limit.child().child();
                            return !agg.getGroupByExpressions().isEmpty() && !agg.getSourceRepeat().isPresent();
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
                            return !agg.getGroupByExpressions().isEmpty() && !agg.getSourceRepeat().isPresent();
                        })
                        .then(topn -> {
                            LogicalAggregate<? extends Plan> agg = topn.child();
                            Pair<List<OrderKey>, List<Expression>> pair =
                                    supplementOrderKeyByGroupKeyIfCompatible(topn, agg);
                            if (pair != null) {
                                agg = agg.withGroupBy(pair.second);
                                topn = (LogicalTopN) topn.withChildren(agg);
                                topn = (LogicalTopN) topn.withOrderKeys(pair.first);
                            }
                            return topn;
                        }).toRule(RuleType.LIMIT_AGG_TO_TOPN_AGG),
                //topn -> project ->agg: add all group key to sort key, and prune column
                logicalTopN(logicalProject(logicalAggregate()))
                        .when(topn -> ConnectContext.get() != null
                                && ConnectContext.get().getSessionVariable().pushTopnToAgg
                                && ConnectContext.get().getSessionVariable().topnOptLimitThreshold
                                >= topn.getLimit() + topn.getOffset())
                        .when(topn -> {
                            LogicalAggregate<? extends Plan> agg = topn.child().child();
                            return !agg.getGroupByExpressions().isEmpty() && !agg.getSourceRepeat().isPresent();
                        })
                        .then(topn -> {
                            LogicalTopN originTopn = topn;
                            LogicalProject<? extends Plan> project = topn.child();
                            LogicalAggregate<? extends Plan> agg = (LogicalAggregate) project.child();
                            if (!project.isAllSlots()) {
                                /*
                                    topn(orderKey=[a])
                                        +-->project(b as a)
                                           +--> agg(groupKey[b]
                                     =>
                                     topn(orderKey=[b])
                                        +-->project(b as a)
                                           +-->agg(groupKey[b])
                                     and then exchange topn and project
                                 */
                                Map<SlotReference, SlotReference> keyAsKey = new HashMap<>();
                                for (NamedExpression e : project.getProjects()) {
                                    if (e instanceof Alias && e.child(0) instanceof SlotReference) {
                                        keyAsKey.put((SlotReference) e.toSlot(), (SlotReference) e.child(0));
                                    }
                                }
                                List<OrderKey> projectOrderKeys = Lists.newArrayList();
                                boolean hasNew = false;
                                for (OrderKey orderKey : topn.getOrderKeys()) {
                                    if (keyAsKey.containsKey(orderKey.getExpr())) {
                                        projectOrderKeys.add(orderKey.withExpression(keyAsKey.get(orderKey.getExpr())));
                                        hasNew = true;
                                    } else {
                                        projectOrderKeys.add(orderKey);
                                    }
                                }
                                if (hasNew) {
                                    topn = (LogicalTopN) topn.withOrderKeys(projectOrderKeys);
                                }
                            }
                            Pair<List<OrderKey>, List<Expression>> pair =
                                    supplementOrderKeyByGroupKeyIfCompatible(topn, agg);
                            Plan result;
                            if (pair == null) {
                                result = originTopn;
                            } else {
                                agg = agg.withGroupBy(pair.second);
                                topn = (LogicalTopN) topn.withOrderKeys(pair.first);
                                if (isOrderKeysInProject(topn, project)) {
                                    project = (LogicalProject<? extends Plan>) project.withChildren(agg);
                                    topn = (LogicalTopN<LogicalProject<LogicalAggregate<Plan>>>)
                                            topn.withChildren(project);
                                    result = topn;
                                } else {
                                    topn = (LogicalTopN) topn.withChildren(agg);
                                    project = (LogicalProject<? extends Plan>) project.withChildren(topn);
                                    result = project;
                                }
                            }
                            return result;
                        }).toRule(RuleType.LIMIT_AGG_TO_TOPN_AGG)
        );
    }

    private boolean isOrderKeysInProject(LogicalTopN<? extends Plan> topn, LogicalProject project) {
        Set<Slot> projectSlots = project.getOutputSet();
        for (OrderKey orderKey : topn.getOrderKeys()) {
            if (!projectSlots.contains(orderKey.getExpr())) {
                return false;
            }
        }
        return true;
    }

    private List<OrderKey> generateOrderKeyByGroupKey(LogicalAggregate<? extends Plan> agg) {
        return agg.getGroupByExpressions().stream()
                .map(key -> new OrderKey(key, true, false))
                .collect(Collectors.toList());
    }

    /**
     * compatible: if order key is subset of group by keys
     * example:
     * 1. orderKey[a, b], groupKeys[b, a, c]
     *    compatible, return Pair(orderKey[a, b, c], groupKey[a, b, c])
     * 2. orderKey[a, b+1], groupKeys[a, b]
     *    not compatible, return null
     */
    private Pair<List<OrderKey>, List<Expression>> supplementOrderKeyByGroupKeyIfCompatible(
            LogicalTopN<? extends Plan> topn, LogicalAggregate<? extends Plan> agg) {
        Set<Expression> groupKeySet = Sets.newHashSet(agg.getGroupByExpressions());
        List<Expression> orderKeyList = topn.getOrderKeys().stream()
                .map(OrderKey::getExpr).collect(Collectors.toList());
        Set<Expression> orderKeySet = Sets.newHashSet(orderKeyList);
        boolean compatible = groupKeySet.containsAll(orderKeyList);
        if (compatible) {
            List<OrderKey> newOrderKeys = Lists.newArrayList(topn.getOrderKeys());
            List<Expression> newGroupExpressions = Lists.newArrayListWithCapacity(agg.getGroupByExpressions().size());
            for (OrderKey orderKey : newOrderKeys) {
                newGroupExpressions.add(orderKey.getExpr());
            }

            for (Expression groupKey : agg.getGroupByExpressions()) {
                if (!orderKeySet.contains(groupKey)) {
                    newOrderKeys.add(new OrderKey(groupKey, true, false));
                    newGroupExpressions.add(groupKey);
                }
            }
            return Pair.of(newOrderKeys, newGroupExpressions);
        } else {
            return null;
        }
    }
}
