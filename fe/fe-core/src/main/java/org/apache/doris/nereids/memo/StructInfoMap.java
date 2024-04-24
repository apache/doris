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

package org.apache.doris.nereids.memo;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Representation for group in cascades optimizer.
 */
public class StructInfoMap {
    private final Map<BitSet, Pair<GroupExpression, List<BitSet>>> groupExpressionMap = new HashMap<>();
    private final Map<BitSet, StructInfo> infoMap = new HashMap<>();
    private boolean refreshed;

    /**
     * get struct info according to table map
     *
     * @param mvTableMap the original table map
     * @param foldTableMap the fold table map
     * @param group the group that the mv matched
     * @return struct info or null if not found
     */
    public @Nullable StructInfo getStructInfo(BitSet mvTableMap, BitSet foldTableMap, Group group, Plan originPlan) {
        if (!infoMap.containsKey(mvTableMap)) {
            if ((groupExpressionMap.containsKey(foldTableMap) || groupExpressionMap.isEmpty())
                    && !groupExpressionMap.containsKey(mvTableMap)) {
                refresh(group);
            }
            if (groupExpressionMap.containsKey(mvTableMap)) {
                Pair<GroupExpression, List<BitSet>> groupExpressionBitSetPair = getGroupExpressionWithChildren(
                        mvTableMap);
                StructInfo structInfo = constructStructInfo(groupExpressionBitSetPair.first,
                        groupExpressionBitSetPair.second, mvTableMap, originPlan);
                infoMap.put(mvTableMap, structInfo);
            }
        }
        return infoMap.get(mvTableMap);
    }

    public Set<BitSet> getTableMaps() {
        return groupExpressionMap.keySet();
    }

    public Collection<StructInfo> getStructInfos() {
        return infoMap.values();
    }

    public Pair<GroupExpression, List<BitSet>> getGroupExpressionWithChildren(BitSet tableMap) {
        return groupExpressionMap.get(tableMap);
    }

    public boolean isRefreshed() {
        return refreshed;
    }

    public void setRefreshed(boolean refreshed) {
        this.refreshed = refreshed;
    }

    private StructInfo constructStructInfo(GroupExpression groupExpression, List<BitSet> children,
            BitSet tableMap, Plan originPlan) {
        // this plan is not origin plan, should record origin plan in struct info
        Plan plan = constructPlan(groupExpression, children, tableMap);
        return originPlan == null ? StructInfo.of(plan) : StructInfo.of(plan, originPlan);
    }

    private Plan constructPlan(GroupExpression groupExpression, List<BitSet> children, BitSet tableMap) {
        List<Plan> childrenPlan = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            StructInfoMap structInfoMap = groupExpression.child(i).getstructInfoMap();
            BitSet childMap = children.get(i);
            Pair<GroupExpression, List<BitSet>> groupExpressionBitSetPair
                    = structInfoMap.getGroupExpressionWithChildren(childMap);
            childrenPlan.add(
                    constructPlan(groupExpressionBitSetPair.first, groupExpressionBitSetPair.second, childMap));
        }
        return groupExpression.getPlan().withChildren(childrenPlan);
    }

    /**
     * refresh group expression map
     *
     * @param group the root group
     *
     * @return whether groupExpressionMap is updated
     */
    public boolean refresh(Group group) {
        Set<Group> refreshedGroup = new HashSet<>();
        int originSize = groupExpressionMap.size();
        for (GroupExpression groupExpression : group.getLogicalExpressions()) {
            List<Set<BitSet>> childrenTableMap = new ArrayList<>();
            boolean needRefresh = groupExpressionMap.isEmpty();
            if (groupExpression.children().isEmpty()) {
                BitSet leaf = constructLeaf(groupExpression);
                groupExpressionMap.put(leaf, Pair.of(groupExpression, new ArrayList<>()));
                continue;
            }

            for (Group child : groupExpression.children()) {
                if (!refreshedGroup.contains(child) && !child.getstructInfoMap().isRefreshed()) {
                    StructInfoMap childStructInfoMap = child.getstructInfoMap();
                    needRefresh |= childStructInfoMap.refresh(child);
                    childStructInfoMap.setRefreshed(true);
                }
                refreshedGroup.add(child);
                childrenTableMap.add(child.getstructInfoMap().getTableMaps());
            }
            // if cumulative child table map is different from current
            // or current group expression map is empty, should update the groupExpressionMap currently
            Collection<Pair<BitSet, List<BitSet>>> bitSetWithChildren = cartesianProduct(childrenTableMap);
            if (needRefresh) {
                for (Pair<BitSet, List<BitSet>> bitSetWithChild : bitSetWithChildren) {
                    groupExpressionMap.putIfAbsent(bitSetWithChild.first,
                            Pair.of(groupExpression, bitSetWithChild.second));
                }
            }
        }
        return originSize != groupExpressionMap.size();
    }

    private BitSet constructLeaf(GroupExpression groupExpression) {
        Plan plan = groupExpression.getPlan();
        BitSet tableMap = new BitSet();
        if (plan instanceof LogicalCatalogRelation) {
            // TODO: Bitset is not compatible with long, use tree map instead
            tableMap.set((int) ((LogicalCatalogRelation) plan).getTable().getId());
        }
        // one row relation / CTE consumer
        return tableMap;
    }

    private Collection<Pair<BitSet, List<BitSet>>> cartesianProduct(List<Set<BitSet>> childrenTableMap) {
        Set<List<BitSet>> cartesianLists = Sets.cartesianProduct(childrenTableMap);
        List<Pair<BitSet, List<BitSet>>> resultPairSet = new LinkedList<>();
        for (List<BitSet> bitSetList : cartesianLists) {
            BitSet bitSet = new BitSet();
            for (BitSet b : bitSetList) {
                bitSet.or(b);
            }
            resultPairSet.add(Pair.of(bitSet, bitSetList));
        }
        return resultPairSet;
    }

    @Override
    public String toString() {
        return "StructInfoMap{ groupExpressionMap = " + groupExpressionMap + ", infoMap = " + infoMap + '}';
    }
}
