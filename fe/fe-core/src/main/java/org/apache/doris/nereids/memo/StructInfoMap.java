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
    private final Map<BitSet, GroupExpression> groupExpressionMap = new HashMap<>();
    private final Map<BitSet, StructInfo> infoMap = new HashMap<>();

    /**
     * get struct info according to table map
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
                StructInfo structInfo = constructStructInfo(groupExpressionMap.get(mvTableMap));
                infoMap.put(mvTableMap, structInfo);
            }
        }

        return infoMap.get(mvTableMap);
    }

    public Set<BitSet> getTableMaps() {
        return groupExpressionMap.keySet();
    }

    private StructInfo constructStructInfo(GroupExpression groupExpression) {
        throw new RuntimeException("has not been implemented for" + groupExpression);
    }

    /**
     * refresh group expression map
     * @param group the root group
     */
    public void refresh(Group group) {
        List<Set<BitSet>> childrenTableMap = new ArrayList<>();
        Set<Group> refreshedGroup = new HashSet<>();
        for (GroupExpression groupExpression : group.getLogicalExpressions()) {
            if (groupExpression.children().isEmpty()) {
                groupExpressionMap.put(constructLeaf(groupExpression), groupExpression);
                continue;
            }
            for (Group child : groupExpression.children()) {
                if (!refreshedGroup.contains(child)) {
                    StructInfoMap childStructInfoMap = child.getstructInfoMap();
                    childStructInfoMap.refresh(child);
                }
                refreshedGroup.add(child);
                childrenTableMap.add(child.getstructInfoMap().getTableMaps());
            }
            Set<BitSet> bitSets = cartesianProduct(childrenTableMap);
            for (BitSet bitSet : bitSets) {
                groupExpressionMap.put(bitSet, groupExpression);
            }
        }
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

    private Set<BitSet> cartesianProduct(List<Set<BitSet>> childrenTableMap) {
        return Sets.cartesianProduct(childrenTableMap)
                .stream()
                .map(bitSetList -> {
                    BitSet bitSet = new BitSet();
                    for (BitSet b : bitSetList) {
                        bitSet.or(b);
                    }
                    return bitSet;
                })
                .collect(Collectors.toSet());
    }
}
