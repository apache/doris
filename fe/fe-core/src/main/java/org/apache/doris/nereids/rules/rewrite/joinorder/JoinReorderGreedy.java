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

package org.apache.doris.nereids.rules.rewrite.joinorder;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.rules.rewrite.StatsDerive;
import org.apache.doris.nereids.rules.rewrite.StatsDerive.DeriveContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**JoinReorderGreedy*/
public class JoinReorderGreedy extends JoinOrder {
    protected final MinMaxPriorityQueue<ExpressionInfo> topKExpr;

    /**JoinReorderGreedy*/
    public JoinReorderGreedy() {
        // Ensure that topk's ExpressionInfo is returned in the same order, the final plan will be different if the
        // cost is same and get ExpressionInfo in different order.
        this.topKExpr = MinMaxPriorityQueue.orderedBy((Comparator<ExpressionInfo>) (left, right) -> {
            double leftCost = left.cost;
            double rightCost = right.cost;
            int result = Double.compare(leftCost, rightCost);
            if (result == 0) {
                return Double.compare(left.hashCode(), right.hashCode());
            } else {
                return result;
            }
        }).maximumSize(10).create();
    }

    @Override
    protected void enumerate() {
        for (int curJoinLevel = 2; curJoinLevel <= atomSize; curJoinLevel++) {
            searchJoinOrders(curJoinLevel - 1, 1, false);
            searchBushyJoinOrders(curJoinLevel);
        }
    }

    @Override
    public List<Plan> getResult() {
        List<Plan> result = Lists.newArrayList();
        while (!topKExpr.isEmpty()) {
            result.add(topKExpr.pollFirst().expr);
        }
        return result;
    }

    private void searchBushyJoinOrders(int curJoinLevel) {
        // Search bushy joins tree fro level x and y, where
        // x + y = curJoinLevel and x > 1 and y > 1 and x >= y.
        // Note that join trees of level 3 and below are never bushy,
        // so this loop only executes at curJoinLevel >= 4
        for (int rightLevel = 2; rightLevel <= curJoinLevel / 2; rightLevel++) {
            searchJoinOrders(curJoinLevel - rightLevel, rightLevel, true);
        }
    }

    protected List<GroupInfo> getGroupForLevel(int level) {
        return joinLevels.get(level).groups;
    }

    private List<GroupInfo> getBestGroupList(List<GroupInfo> groupInfos, JoinLevel curLevel) {
        // Do not use greedy algorithms to select the first table, otherwise it is easy to fall into local optimality
        if (curLevel.level == 1) {
            return groupInfos;
        } else {
            Set<GroupInfo> bestGroupInfos = Sets.newHashSet();
            // Get join level 1 used atoms
            List<BitSet> levelOneGroups = Lists.newArrayList();
            getGroupForLevel(1).forEach(groupInfo -> levelOneGroups.add(groupInfo.atoms));
            // For each atom, choose at least one group info to return.
            for (BitSet levelOneGroup : levelOneGroups) {
                List<GroupInfo> candidateGroups = groupInfos.stream().filter(
                                groupInfo -> groupInfo.atoms.intersects(levelOneGroup)
                                        && !bestGroupInfos.contains(groupInfo))
                        .collect(Collectors.toList());
                // Get best group info from candidate group info
                if (!candidateGroups.isEmpty()) {
                    bestGroupInfos.add(getBestGroupInfo(candidateGroups));
                }
            }
            return Lists.newArrayList(bestGroupInfos);
        }
    }

    private GroupInfo getBestGroupInfo(List<GroupInfo> groupInfos) {
        double bestCost = Double.MAX_VALUE;
        GroupInfo bestExpr = null;
        for (GroupInfo groupInfo : groupInfos) {
            if (groupInfo.bestExprInfo.cost < bestCost) {
                bestExpr = groupInfo;
                bestCost = groupInfo.bestExprInfo.cost;
            }
        }
        return bestExpr;
    }

    private void searchJoinOrders(int leftLevel, int rightLevel, boolean isSearchBushyJoin) {
        List<GroupInfo> leftGroupInfos = getGroupForLevel(leftLevel);
        List<GroupInfo> rightGroupInfos = getGroupForLevel(rightLevel);
        JoinLevel curLevel = joinLevels.get(leftLevel + rightLevel);
        if (isSearchBushyJoin) {
            rightGroupInfos = getBestGroupList(rightGroupInfos, curLevel);
        }
        List<GroupInfo> bestLeftGroups = getBestGroupList(leftGroupInfos, curLevel);
        for (GroupInfo leftGroup : bestLeftGroups) {
            BitSet leftBitset = leftGroup.atoms;
            double bestCost = Double.MAX_VALUE;

            for (GroupInfo rightGroup : rightGroupInfos) {
                BitSet rightBitset = rightGroup.atoms;
                if (leftBitset.intersects(rightBitset)) {
                    continue;
                }

                Optional<ExpressionInfo> joinExpr = buildJoinExpr(leftGroup, rightGroup);
                if (!joinExpr.isPresent()) {
                    continue;
                }
                joinExpr.get().expr.accept(new StatsDerive(false), new DeriveContext());

                BitSet joinBitSet = new BitSet();
                joinBitSet.or(leftBitset);
                joinBitSet.or(rightBitset);

                computeCost(joinExpr.get());
                getOrCreateGroupInfo(curLevel, joinBitSet, joinExpr.get());
                double joinCost = joinExpr.get().cost;
                if (joinCost < bestCost) {
                    bestCost = joinCost;
                }
            }
        }
    }

    protected Optional<ExpressionInfo> buildJoinExpr(GroupInfo leftGroup, GroupInfo rightGroup) {
        // 1.找join的条件
        // 目前存在的问题是，没有判断onPredicates里是否是等值条件
        List<Expression> onPredicates = buildInnerJoinPredicate(leftGroup.atoms, rightGroup.atoms);
        // 2.判断左右
        ExpressionInfo leftExprInfo = leftGroup.bestExprInfo;
        ExpressionInfo rightExprInfo = rightGroup.bestExprInfo;
        Plan leftChildPlan;
        Plan rightChildPlan;
        boolean needReverse = false;
        if (leftExprInfo.rowCount < rightExprInfo.rowCount) {
            // 需要交换
            needReverse = true;
            leftChildPlan = rightExprInfo.expr;
            rightChildPlan = leftExprInfo.expr;
        } else {
            // 不需要交换
            leftChildPlan = leftExprInfo.expr;
            rightChildPlan = rightExprInfo.expr;
        }
        LogicalJoin<Plan, Plan> join;
        if (onPredicates.isEmpty()) {
            join = new LogicalJoin(JoinType.CROSS_JOIN, onPredicates, leftChildPlan, rightChildPlan,
                    new JoinReorderContext());
        } else {
            Pair<List<Expression>, List<Expression>> pair = JoinUtils.extractExpressionForHashTable(
                    leftChildPlan.getOutput(), rightChildPlan.getOutput(), onPredicates);
            join = new LogicalJoin(JoinType.INNER_JOIN, pair.first, pair.second, leftChildPlan, rightChildPlan,
                    new JoinReorderContext());
        }
        return Optional.of(needReverse ? new ExpressionInfo(join, rightGroup, leftGroup)
                : new ExpressionInfo(join, leftGroup, rightGroup));
    }

    protected GroupInfo getOrCreateGroupInfo(JoinLevel joinLevel, BitSet atoms,
            ExpressionInfo exprInfo) {
        GroupInfo groupInfo;
        if (bitSetToGroupInfo.containsKey(atoms)) {
            groupInfo = bitSetToGroupInfo.get(atoms);
        } else {
            groupInfo = new GroupInfo(atoms);
            joinLevel.groups.add(groupInfo);
            if (joinLevel.level > 1) {
                bitSetToGroupInfo.put(groupInfo.atoms, groupInfo);
            }
        }

        if (groupInfo.bestExprInfo == null || groupInfo.bestExprInfo != exprInfo) {
            addExprToGroup(groupInfo, exprInfo);
        }
        return groupInfo;
    }

    protected void addExprToGroup(GroupInfo groupInfo, ExpressionInfo expr) {
        Preconditions.checkState(expr.cost != -1);
        double cost = expr.cost;
        // For top group, we keep multi best join expressions
        if (groupInfo.atoms.cardinality() == atomSize) {
            // avoid repeated put, check object is enough
            if (!topKExpr.contains(expr)) {
                topKExpr.offer(expr);
            }
        } else {
            if (cost < groupInfo.lowestExprCost) {
                groupInfo.bestExprInfo = expr;
                groupInfo.lowestExprCost = cost;
            }
        }
    }
}
