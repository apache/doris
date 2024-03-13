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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Representation for group in cascades optimizer.
 */
public class StructInfoMap {
    private final Map<BitSet, Pair<GroupExpression, List<BitSet>>> groupExpressionMap = new HashMap<>();
    private final Map<BitSet, StructInfo> infoMap = new HashMap<>();

    /**
     * get struct info according to table map
     *
     * @param mvTableMap the original table map
     * @param foldTableMap the fold table map
     * @param group the group that the mv matched
     * @return struct info or null if not found
     */
    public @Nullable StructInfo getStructInfo(BitSet mvTableMap, BitSet foldTableMap, Group group) {
        if (!infoMap.containsKey(mvTableMap)) {
            if ((groupExpressionMap.containsKey(foldTableMap) || groupExpressionMap.isEmpty())
                    && !groupExpressionMap.containsKey(mvTableMap)) {
                refresh(group);
            }
            if (groupExpressionMap.containsKey(mvTableMap)) {
                Pair<GroupExpression, List<BitSet>> groupExpressionBitSetPair = getGroupExpressionWithChildren(
                        mvTableMap);
                StructInfo structInfo = constructStructInfo(groupExpressionBitSetPair.first,
                        groupExpressionBitSetPair.second, mvTableMap);
                infoMap.put(mvTableMap, structInfo);
            }
        }

        return infoMap.get(mvTableMap);
    }

    public Set<BitSet> getTableMaps() {
        return groupExpressionMap.keySet();
    }

    public Pair<GroupExpression, List<BitSet>> getGroupExpressionWithChildren(BitSet tableMap) {
        return groupExpressionMap.get(tableMap);
    }

    private StructInfo constructStructInfo(GroupExpression groupExpression, List<BitSet> children, BitSet tableMap) {
        Plan plan = constructPlan(groupExpression, children, tableMap);
        return StructInfo.of(plan).get(0);
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
            boolean needRefresh = false;
            if (groupExpression.children().isEmpty()) {
                BitSet leaf = constructLeaf(groupExpression);
                groupExpressionMap.put(leaf, Pair.of(groupExpression, new ArrayList<>()));
                continue;
            }

            for (Group child : groupExpression.children()) {
                if (!refreshedGroup.contains(child)) {
                    StructInfoMap childStructInfoMap = child.getstructInfoMap();
                    needRefresh |= childStructInfoMap.refresh(child);
                }
                refreshedGroup.add(child);
                childrenTableMap.add(child.getstructInfoMap().getTableMaps());
            }

            if (needRefresh) {
                Set<Pair<BitSet, List<BitSet>>> bitSetWithChildren = cartesianProduct(childrenTableMap);
                for (Pair<BitSet, List<BitSet>> bitSetWithChild : bitSetWithChildren) {
                    groupExpressionMap.put(bitSetWithChild.first, Pair.of(groupExpression, bitSetWithChild.second));
                }
            }
        }
        return originSize != groupExpressionMap.size();
    }

    private BitSet constructLeaf(GroupExpression groupExpression) {
        Plan plan = groupExpression.getPlan();
        BitSet tableMap = new BitSet();
        if (plan instanceof LogicalCatalogRelation) {
            // TODO: Bitmap is not compatible with long, use tree map instead
            tableMap.set((int) ((LogicalCatalogRelation) plan).getTable().getId());
        }
        // one row relation / CTE consumer
        return tableMap;
    }

    private Set<Pair<BitSet, List<BitSet>>> cartesianProduct(List<Set<BitSet>> childrenTableMap) {
        return Sets.cartesianProduct(childrenTableMap)
                .stream()
                .map(bitSetList -> {
                    BitSet bitSet = new BitSet();
                    for (BitSet b : bitSetList) {
                        bitSet.or(b);
                    }
                    return Pair.of(bitSet, bitSetList);
                })
                .collect(Collectors.toSet());
    }
}
