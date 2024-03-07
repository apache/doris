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

import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Eliminate group by key based on fd item information.
 */
public class EliminateGroupByKey extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate(logicalProject()).then(agg -> {
            Set<Integer> enableNereidsRules = ConnectContext.get().getSessionVariable().getEnableNereidsRules();
            if (!enableNereidsRules.contains(RuleType.ELIMINATE_GROUP_BY_KEY.type())) {
                return null;
            }
            LogicalPlan childPlan = agg.child();
            List<FdItem> uniqueFdItems = new ArrayList<>();
            List<FdItem> nonUniqueFdItems = new ArrayList<>();
            if (agg.getGroupByExpressions().isEmpty()
                    || !agg.getGroupByExpressions().stream().allMatch(e -> e instanceof SlotReference)) {
                return null;
            }
            ImmutableSet<FdItem> fdItems = childPlan.getLogicalProperties().getFunctionalDependencies().getFdItems();
            if (fdItems.isEmpty()) {
                return null;
            }
            List<SlotReference> candiExprs = agg.getGroupByExpressions().stream()
                    .map(SlotReference.class::cast).collect(Collectors.toList());

            fdItems.stream().filter(e -> !e.isCandidate()).forEach(e -> {
                        if (e.isUnique()) {
                            uniqueFdItems.add(e);
                        } else {
                            nonUniqueFdItems.add(e);
                        }
                    }
            );

            int minParentExprCnt = -1;
            ImmutableSet<SlotReference> minParentExprs = ImmutableSet.of();
            // if unique fd items exists, try to find the one which has the
            // smallest parent exprs
            for (int i = 0; i < uniqueFdItems.size(); i++) {
                FdItem fdItem = uniqueFdItems.get(i);
                ImmutableSet<SlotReference> parentExprs = fdItem.getParentExprs();
                if (minParentExprCnt == -1 || parentExprs.size() < minParentExprCnt) {
                    boolean isContain = isExprsContainFdParent(candiExprs, fdItem);
                    if (isContain) {
                        minParentExprCnt = parentExprs.size();
                        minParentExprs = ImmutableSet.copyOf(parentExprs);
                    }
                }
            }

            Set<Integer> rootExprsSet = new HashSet<>();
            List<SlotReference> rootExprs = new ArrayList<>();
            Set<Integer> eliminateSet = new HashSet<>();
            if (minParentExprs.size() > 0) {
                // if any unique fd item found, find the expr which matching parentExprs
                // from candiExprs directly
                for (int i = 0; i < minParentExprs.size(); i++) {
                    int index = findEqualExpr(candiExprs, minParentExprs.asList().get(i));
                    if (index != -1) {
                        rootExprsSet.add(new Integer(index));
                    } else {
                        return null;
                    }
                }
            } else {
                // no unique fd item found, try to find the smallest root exprs set
                // from non-unique fd items.
                for (int i = 0; i < nonUniqueFdItems.size() && eliminateSet.size() < candiExprs.size(); i++) {
                    FdItem fdItem = nonUniqueFdItems.get(i);
                    ImmutableSet<SlotReference> parentExprs = fdItem.getParentExprs();
                    boolean isContains = isExprsContainFdParent(candiExprs, fdItem);
                    if (isContains) {
                        List<SlotReference> leftDomain = new ArrayList<>();
                        List<SlotReference> rightDomain = new ArrayList<>();
                        // generate new root exprs
                        for (int j = 0; j < rootExprs.size(); j++) {
                            leftDomain.add(rootExprs.get(j));
                            boolean isInChild = fdItem.checkExprInChild(rootExprs.get(j), childPlan);
                            if (!isInChild) {
                                rightDomain.add(rootExprs.get(j));
                            }
                        }
                        for (int j = 0; j < parentExprs.size(); j++) {
                            int index = findEqualExpr(candiExprs, parentExprs.asList().get(j));
                            if (index != -1) {
                                rightDomain.add(candiExprs.get(index));
                                if (!eliminateSet.contains(index)) {
                                    leftDomain.add(candiExprs.get(index));
                                }
                            }
                        }
                        // check fd can eliminate new candi expr
                        for (int j = 0; j < candiExprs.size(); j++) {
                            if (!eliminateSet.contains(j)) {
                                boolean isInChild = fdItem.checkExprInChild(candiExprs.get(j), childPlan);
                                if (isInChild) {
                                    eliminateSet.add(j);
                                }
                            }
                        }
                        // if fd eliminate new candi exprs or new root exprs is smaller than the older,
                        // than use new root expr to replace old ones
                        List<SlotReference> newRootExprs = leftDomain.size() <= rightDomain.size()
                                ? leftDomain : rightDomain;
                        rootExprs.clear();
                        rootExprs.addAll(newRootExprs);
                    }
                }
            }
            // find the root expr, add into root exprs set, indicate the index in
            // candiExprs list
            for (int i = 0; i < rootExprs.size(); i++) {
                int index = findEqualExpr(candiExprs, rootExprs.get(i));
                if (index != -1) {
                    rootExprsSet.add(new Integer(index));
                } else {
                    return null;
                }
            }
            // other can't be determined expr, add into root exprs directly
            if (eliminateSet.size() < candiExprs.size()) {
                for (int i = 0; i < candiExprs.size(); i++) {
                    if (!eliminateSet.contains(i)) {
                        rootExprsSet.add(i);
                    }
                }
            }
            rootExprs.clear();
            for (int i = 0; i < candiExprs.size(); i++) {
                if (rootExprsSet.contains(i)) {
                    rootExprs.add(candiExprs.get(i));
                }
            }

            // use the new rootExprs as new group by keys
            List<Expression> resultExprs = new ArrayList<>();
            for (int i = 0; i < rootExprs.size(); i++) {
                resultExprs.add(rootExprs.get(i));
            }

            // eliminate outputs keys
            // TODO: remove outputExprList computing
            List<NamedExpression> outputExprList = new ArrayList<>();
            for (int i = 0; i < agg.getOutputExpressions().size(); i++) {
                if (rootExprsSet.contains(i)) {
                    outputExprList.add(agg.getOutputExpressions().get(i));
                }
            }
            // find the remained outputExprs list
            List<NamedExpression> remainedOutputExprList = new ArrayList<>();
            for (int i = 0; i < agg.getOutputExpressions().size(); i++) {
                NamedExpression outputExpr = agg.getOutputExpressions().get(i);
                if (!agg.getGroupByExpressions().contains(outputExpr)) {
                    remainedOutputExprList.add(outputExpr);
                }
            }
            outputExprList.addAll(remainedOutputExprList);
            return new LogicalAggregate<>(resultExprs, agg.getOutputExpressions(), agg.child());
        }).toRule(RuleType.ELIMINATE_GROUP_BY_KEY);
    }

    /**
     * find the equal expr index from expr list.
     */
    public int findEqualExpr(List<SlotReference> exprList, SlotReference expr) {
        for (int i = 0; i < exprList.size(); i++) {
            if (exprList.get(i).equals(expr)) {
                return i;
            }
        }
        return -1;
    }

    private boolean isExprsContainFdParent(List<SlotReference> candiExprs, FdItem fdItem) {
        return fdItem.getParentExprs().stream().allMatch(e -> candiExprs.contains(e));
    }
}
