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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Representation for group in cascades optimizer.
 */
public class StructInfoMap {

    public static final Logger LOG = LogManager.getLogger(StructInfoMap.class);
    /**
     * The map key is the relation id bit set to get corresponding plan accurately
     */
    private final Map<BitSet, Pair<GroupExpression, List<BitSet>>> groupExpressionMap = new HashMap<>();
    /**
     * The map key is the relation id bit set to get corresponding plan accurately
     */
    private final Map<BitSet, StructInfo> infoMap = new HashMap<>();
    private long refreshVersion = 0;

    /**
     * get struct info according to table map
     *
     * @param tableMap the original table map
     * @param group the group that the mv matched
     * @return struct info or null if not found
     */
    public @Nullable StructInfo getStructInfo(CascadesContext cascadesContext, BitSet tableMap, Group group,
            Plan originPlan, boolean forceRefresh) {
        StructInfo structInfo = infoMap.get(tableMap);
        if (structInfo != null) {
            return structInfo;
        }
        if (groupExpressionMap.isEmpty() || !groupExpressionMap.containsKey(tableMap)) {
            refresh(group, cascadesContext, tableMap, new HashSet<>(),
                    forceRefresh);
            group.getStructInfoMap().setRefreshVersion(cascadesContext.getMemo().getRefreshVersion());
        }
        if (groupExpressionMap.containsKey(tableMap)) {
            Pair<GroupExpression, List<BitSet>> groupExpressionBitSetPair = getGroupExpressionWithChildren(tableMap);
            structInfo = constructStructInfo(groupExpressionBitSetPair.first, groupExpressionBitSetPair.second,
                    originPlan, cascadesContext);
            infoMap.put(tableMap, structInfo);
        }
        return structInfo;
    }

    public Set<BitSet> getTableMaps() {
        return groupExpressionMap.keySet();
    }

    public Pair<GroupExpression, List<BitSet>> getGroupExpressionWithChildren(BitSet tableMap) {
        return groupExpressionMap.get(tableMap);
    }

    public void setRefreshVersion(long refreshVersion) {
        this.refreshVersion = refreshVersion;
    }

    private StructInfo constructStructInfo(GroupExpression groupExpression, List<BitSet> children,
            Plan originPlan, CascadesContext cascadesContext) {
        // this plan is not origin plan, should record origin plan in struct info
        Plan plan = constructPlan(groupExpression, children);
        return originPlan == null ? StructInfo.of(plan, cascadesContext)
                : StructInfo.of(plan, originPlan, cascadesContext);
    }

    private Plan constructPlan(GroupExpression groupExpression, List<BitSet> children) {
        List<Plan> childrenPlan = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            StructInfoMap structInfoMap = groupExpression.child(i).getStructInfoMap();
            BitSet childMap = children.get(i);
            Pair<GroupExpression, List<BitSet>> groupExpressionBitSetPair
                    = structInfoMap.getGroupExpressionWithChildren(childMap);
            childrenPlan.add(
                    constructPlan(groupExpressionBitSetPair.first, groupExpressionBitSetPair.second));
        }
        // need to clear current group expression info by using withGroupExpression
        // this plan would copy into memo, if with group expression, would cause err
        return groupExpression.getPlan().withChildren(childrenPlan).withGroupExpression(Optional.empty());
    }

    /**
     * refresh group expression map
     *
     * @param group the root group
     * @param targetBitSet refreshed group expression table bitset must intersect with the targetBitSet
     *
     */
    public void refresh(Group group, CascadesContext cascadesContext,
            BitSet targetBitSet,
            Set<Integer> refreshedGroup,
            boolean forceRefresh) {
        StructInfoMap structInfoMap = group.getStructInfoMap();
        refreshedGroup.add(group.getGroupId().asInt());
        long memoVersion = cascadesContext.getMemo().getRefreshVersion();
        if (!structInfoMap.getTableMaps().isEmpty() && memoVersion == structInfoMap.refreshVersion) {
            return;
        }
        for (GroupExpression groupExpression : group.getLogicalExpressions()) {
            List<Set<BitSet>> childrenTableMap = new LinkedList<>();
            if (groupExpression.children().isEmpty()) {
                BitSet leaf = constructLeaf(groupExpression, cascadesContext, forceRefresh);
                if (leaf.isEmpty()) {
                    break;
                }
                groupExpressionMap.put(leaf, Pair.of(groupExpression, new LinkedList<>()));
                continue;
            }
            for (Group child : groupExpression.children()) {
                StructInfoMap childStructInfoMap = child.getStructInfoMap();
                if (!refreshedGroup.contains(child.getGroupId().asInt())) {
                    childStructInfoMap.refresh(child, cascadesContext, targetBitSet, refreshedGroup, forceRefresh);
                    childStructInfoMap.setRefreshVersion(memoVersion);
                }
                Set<BitSet> filteredTableMaps = new HashSet<>();
                for (BitSet tableMaps : child.getStructInfoMap().getTableMaps()) {
                    // filter the tableSet that used intersects with targetBitSet
                    if (!targetBitSet.isEmpty() && !tableMaps.intersects(targetBitSet)) {
                        continue;
                    }
                    filteredTableMaps.add(tableMaps);
                }
                if (!filteredTableMaps.isEmpty()) {
                    childrenTableMap.add(filteredTableMaps);
                }
            }
            if (childrenTableMap.isEmpty()) {
                continue;
            }
            // if groupExpression which has the same table set have refreshed, continue
            BitSet eachGroupExpressionTableSet = new BitSet();
            for (Set<BitSet> groupExpressionBitSet : childrenTableMap) {
                for (BitSet bitSet : groupExpressionBitSet) {
                    eachGroupExpressionTableSet.or(bitSet);
                }
            }
            if (groupExpressionMap.containsKey(eachGroupExpressionTableSet)) {
                // for the group expressions of group, only need to refresh any of the group expression
                // when they have the same group expression table set
                continue;
            }
            // if cumulative child table map is different from current
            // or current group expression map is empty, should update the groupExpressionMap currently
            Collection<Pair<BitSet, List<BitSet>>> bitSetWithChildren = cartesianProduct(childrenTableMap);
            for (Pair<BitSet, List<BitSet>> bitSetWithChild : bitSetWithChildren) {
                groupExpressionMap.putIfAbsent(bitSetWithChild.first,
                        Pair.of(groupExpression, bitSetWithChild.second));
            }

        }
    }

    private BitSet constructLeaf(GroupExpression groupExpression, CascadesContext cascadesContext,
            boolean forceRefresh) {
        Plan plan = groupExpression.getPlan();
        BitSet tableMap = new BitSet();
        if (plan instanceof LogicalCatalogRelation) {
            LogicalCatalogRelation logicalCatalogRelation = (LogicalCatalogRelation) plan;
            TableIf table = logicalCatalogRelation.getTable();
            // If disable materialized view nest rewrite, and mv already rewritten successfully once, doesn't construct
            // table id map for nest mv rewrite
            if (!forceRefresh && cascadesContext.getStatementContext()
                    .getMaterializationRewrittenSuccessSet().contains(table.getFullQualifiers())) {
                return tableMap;
            }
            tableMap.set(logicalCatalogRelation.getRelationId().asInt());
        }
        // one row relation / CTE consumer
        if (plan instanceof LogicalCTEConsumer || plan instanceof LogicalEmptyRelation
                || plan instanceof LogicalOneRowRelation) {
            tableMap.set(((LogicalRelation) plan).getRelationId().asInt());
        }
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
