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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**JoinReorderGreedy*/
public class JoinReorderGreedy extends JoinOrder {
    @Override
    protected void enumerate() {
        for (int curJoinLevel = 2; curJoinLevel <= atomSize; curJoinLevel++) {
            searchJoinOrders(curJoinLevel - 1, 1, false);
            searchBushyJoinOrders(curJoinLevel);
        }
    }

    @Override
    public List<Plan> getResult() {
        BitSet bitSet = new BitSet();
        bitSet.set(0, atomSize);
        GroupInfo group = bitSetToGroupInfo.get(bitSet);
        if (group == null) {
            return ImmutableList.of();
        }
        return ImmutableList.of(group.bestPlanInfo.plan);
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
            Set<GroupInfo> bestGroupInfos = Sets.newLinkedHashSet();
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
        GroupInfo bestPlan = null;
        for (GroupInfo groupInfo : groupInfos) {
            if (groupInfo.bestPlanInfo.cost < bestCost) {
                bestPlan = groupInfo;
                bestCost = groupInfo.bestPlanInfo.cost;
            }
        }
        return bestPlan;
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
            for (GroupInfo rightGroup : rightGroupInfos) {
                BitSet rightBitset = rightGroup.atoms;
                if (leftBitset.intersects(rightBitset)) {
                    continue;
                }

                Optional<PlanInfo> join = buildJoin(leftGroup, rightGroup);
                if (!join.isPresent()) {
                    continue;
                }
                join.get().plan.accept(new StatsDerive(false), new DeriveContext());

                BitSet joinBitSet = new BitSet();
                joinBitSet.or(leftBitset);
                joinBitSet.or(rightBitset);

                computeCost(join.get());
                getOrCreateGroupInfo(curLevel, joinBitSet, join.get());
            }
        }
    }

    protected Optional<PlanInfo> buildJoin(GroupInfo leftGroup, GroupInfo rightGroup) {
        List<Expression> onPredicates = buildInnerJoinPredicate(leftGroup.atoms, rightGroup.atoms);
        PlanInfo leftPlanInfo = leftGroup.bestPlanInfo;
        PlanInfo rightPlanInfo = rightGroup.bestPlanInfo;
        Plan leftChildPlan;
        Plan rightChildPlan;
        boolean needReverse = false;
        if (leftPlanInfo.rowCount < rightPlanInfo.rowCount) {
            needReverse = true;
            leftChildPlan = rightPlanInfo.plan;
            rightChildPlan = leftPlanInfo.plan;
        } else {
            leftChildPlan = leftPlanInfo.plan;
            rightChildPlan = rightPlanInfo.plan;
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
        return Optional.of(needReverse ? new PlanInfo(join, rightGroup, leftGroup)
                : new PlanInfo(join, leftGroup, rightGroup));
    }

    protected void getOrCreateGroupInfo(JoinLevel joinLevel, BitSet atoms, PlanInfo planInfo) {
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

        if (groupInfo.bestPlanInfo == null || groupInfo.bestPlanInfo != planInfo) {
            addPlanToGroup(groupInfo, planInfo);
        }
    }

    protected void addPlanToGroup(GroupInfo groupInfo, PlanInfo plan) {
        Preconditions.checkState(plan.cost != -1);
        double cost = plan.cost;
        if (cost < groupInfo.lowestPlanCost) {
            groupInfo.bestPlanInfo = plan;
            groupInfo.lowestPlanCost = cost;
        }
    }
}
