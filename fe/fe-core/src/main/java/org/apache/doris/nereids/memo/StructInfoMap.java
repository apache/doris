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
import org.apache.doris.nereids.util.MoreFieldsThread;

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
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Representation for group in cascades optimizer.
 */
public class StructInfoMap {

    public static final Logger LOG = LogManager.getLogger(StructInfoMap.class);
    // 2166136261
    private static final int FNV32_OFFSET_BASIS = 0x811C9DC5;
    // 16777619
    private static final int FNV32_PRIME = 0x01000193;
    /**
     * Strategy for table ID mode
     */
    private static final IdModeStrategy TABLE_ID_STRATEGY = new IdModeStrategy() {
        @Override
        public Map<BitSet, Pair<GroupExpression, List<BitSet>>> getGroupExpressionMap(StructInfoMap structInfoMap) {
            return structInfoMap.groupExpressionMapByTableId;
        }

        @Override
        public Map<BitSet, StructInfo> getInfoMap(StructInfoMap structInfoMap) {
            return structInfoMap.infoMapByTableId;
        }

        @Override
        public BitSet constructLeaf(GroupExpression groupExpression, CascadesContext cascadesContext,
                                    boolean forceRefresh) {
            Plan plan = groupExpression.getPlan();
            BitSet tableMap = new BitSet();
            if (plan instanceof LogicalCatalogRelation) {
                LogicalCatalogRelation relation = (LogicalCatalogRelation) plan;
                TableIf table = relation.getTable();
                if (!forceRefresh && cascadesContext.getStatementContext()
                        .getMaterializationRewrittenSuccessSet().contains(table.getFullQualifiers())) {
                    return tableMap;
                }
                tableMap.set(cascadesContext.getStatementContext().getTableId(table).asInt());
            }
            return tableMap;
        }

        @Override
        public int computeMemoVersion(BitSet targetIdMap, CascadesContext cascadesContext) {
            return getMemoVersion(targetIdMap, cascadesContext.getMemo().getRefreshVersion());
        }
    };

    /**
     * Strategy for relation ID mode
     */
    private static final IdModeStrategy RELATION_ID_STRATEGY = new IdModeStrategy() {
        @Override
        public Map<BitSet, Pair<GroupExpression, List<BitSet>>> getGroupExpressionMap(StructInfoMap structInfoMap) {
            return structInfoMap.groupExpressionMapByRelationId;
        }

        @Override
        public Map<BitSet, StructInfo> getInfoMap(StructInfoMap structInfoMap) {
            return structInfoMap.infoMapByRelationId;
        }

        @Override
        public BitSet constructLeaf(GroupExpression groupExpression, CascadesContext cascadesContext,
                                    boolean forceRefresh) {
            Plan plan = groupExpression.getPlan();
            BitSet tableMap = new BitSet();
            if (plan instanceof LogicalCatalogRelation) {
                LogicalCatalogRelation relation = (LogicalCatalogRelation) plan;
                TableIf table = relation.getTable();
                if (!forceRefresh && cascadesContext.getStatementContext()
                        .getMaterializationRewrittenSuccessSet().contains(table.getFullQualifiers())) {
                    return tableMap;
                }
                tableMap.set(relation.getRelationId().asInt());
            }
            if (plan instanceof LogicalCTEConsumer || plan instanceof LogicalEmptyRelation
                    || plan instanceof LogicalOneRowRelation) {
                tableMap.set(((LogicalRelation) plan).getRelationId().asInt());
            }
            return tableMap;
        }

        @Override
        public int computeMemoVersion(BitSet targetIdMap, CascadesContext cascadesContext) {
            return getMemoVersion(targetIdMap, cascadesContext.getMemo().getRefreshVersion());
        }
    };
    /**
     * The map key is the relation id bit set to get corresponding plan accurately
     */
    private final Map<BitSet, Pair<GroupExpression, List<BitSet>>> groupExpressionMapByRelationId = new HashMap<>();
    /**
     * The map key is the relation id bit set to get corresponding plan accurately
     */
    private final Map<BitSet, StructInfo> infoMapByRelationId = new HashMap<>();

    /**
     * The map key is the common table id bit set to get corresponding plan accurately
     */
    private final Map<BitSet, Pair<GroupExpression, List<BitSet>>> groupExpressionMapByTableId = new HashMap<>();
    /**
     * The map key is the common table id bit set to get corresponding plan accurately
     */
    private final Map<BitSet, StructInfo> infoMapByTableId = new HashMap<>();

    // The key is the tableIds query used, the value is the refresh version when last refresh
    private final Map<BitSet, Integer> refreshVersion = new HashMap<>();

    /**
     * get struct info according to table map
     *
     * @param targetIdMap the original table map
     * @param group the group that the mv matched
     * @return struct info or null if not found
     */
    public @Nullable StructInfo getStructInfo(CascadesContext cascadesContext, BitSet targetIdMap, Group group,
            Plan originPlan, boolean forceRefresh, boolean tableIdMode) {
        IdModeStrategy strategy = getStrategy(tableIdMode);
        Map<BitSet, StructInfo> infoMap = strategy.getInfoMap(this);
        Map<BitSet, Pair<GroupExpression, List<BitSet>>> groupExprMap = strategy.getGroupExpressionMap(this);

        StructInfo structInfo = infoMap.get(targetIdMap);
        if (structInfo != null) {
            return structInfo;
        }
        if (groupExprMap.isEmpty() || !groupExprMap.containsKey(targetIdMap)) {
            int memoVersion = strategy.computeMemoVersion(targetIdMap, cascadesContext);
            refresh(group, cascadesContext, targetIdMap, new HashSet<>(), forceRefresh, memoVersion, tableIdMode);
            group.getStructInfoMap().setRefreshVersion(targetIdMap, cascadesContext.getMemo().getRefreshVersion());
        }
        if (groupExprMap.containsKey(targetIdMap)) {
            Pair<GroupExpression, List<BitSet>> groupExpressionBitSetPair =
                    getGroupExpressionWithChildren(targetIdMap, tableIdMode);
            // NOTICE: During the transition from physicalAggregate to logical aggregation,
            // the original function signature needs to remain unchanged because the constructor
            // of LogicalAggregation will recalculate the signature of the aggregation function.
            // When the calculated signature is inconsistent with the original signature
            // (e.g. due to the influence of the session variable enable_decimal256),
            // a problem will arise where the output type of the rewritten plan is inconsistent with
            // the output type of the upper-level operator.
            structInfo = MoreFieldsThread.keepFunctionSignature(() ->
                    constructStructInfo(groupExpressionBitSetPair.first, groupExpressionBitSetPair.second,
                            originPlan, cascadesContext, tableIdMode));
            infoMap.put(targetIdMap, structInfo);
        }
        return structInfo;
    }

    public Set<BitSet> getTableMaps(boolean tableIdMode) {
        return getStrategy(tableIdMode).getGroupExpressionMap(this).keySet();
    }

    public Pair<GroupExpression, List<BitSet>> getGroupExpressionWithChildren(BitSet tableMap, boolean tableIdMode) {
        return getStrategy(tableIdMode).getGroupExpressionMap(this).get(tableMap);
    }

    // Set the refresh version for the given targetIdSet
    public void setRefreshVersion(BitSet targetIdSet, Map<Integer, AtomicInteger> memoRefreshVersionMap) {
        this.refreshVersion.put(targetIdSet, getMemoVersion(targetIdSet, memoRefreshVersionMap));
    }

    // Set the refresh version for the given targetIdSet
    public void setRefreshVersion(BitSet targetIdSet, int memoRefreshVersion) {
        this.refreshVersion.put(targetIdSet, memoRefreshVersion);
    }

    // Get the refresh version for the given targetIdSet, if not exist, return 0
    public long getRefreshVersion(BitSet targetIdSet) {
        return refreshVersion.computeIfAbsent(targetIdSet, k -> 0);
    }

    /**
     * Compute a compact "version fingerprint" for the given relation id set.
     * Algorithm:
     * - Uses a 32-bit FNV-1a-style hash. Start from FNV32_OFFSET_BASIS and multiply by FNV32_PRIME.
     * - Iterate each set bit (target id) in the BitSet:
     *   - Fetch its current refresh version from memoRefreshVersionMap (default 0 if absent).
     *   - Mix the version into the hash by XOR, then diffuse by multiplying the FNV prime.
     * - Returns the final hash as the memo version for this set of relations.
     * Benefits:
     * - Stable fingerprint: any change in any relation's version produces a different hash, enabling
     *   fast cache invalidation checks without scanning all versions every time.
     * - Order-independent: relies on set iteration; the same set yields the same hash regardless of order.
     * - Low memory and CPU overhead: compresses multiple integers into a single 32-bit value efficiently.
     * - Incremental-friendly: new relations/versions can be incorporated by re-running on the changed set.
     * - Good diffusion: XOR + prime multiplication reduces collisions compared to simple sums.
     * Notes:
     * - The Integer.MAX_VALUE guard prevents potential overflow edge cases in BitSet iteration.
     */
    public static int getMemoVersion(BitSet targetIdSet, Map<Integer, AtomicInteger> memoRefreshVersionMap) {
        int hash = FNV32_OFFSET_BASIS;
        for (int id = targetIdSet.nextSetBit(0);
                id >= 0; id = targetIdSet.nextSetBit(id + 1)) {
            AtomicInteger ver = memoRefreshVersionMap.get(id);
            int tmpVer = ver == null ? 0 : ver.get();
            hash ^= tmpVer;
            hash *= FNV32_PRIME;
            if (id == Integer.MAX_VALUE) {
                break;
            }
        }
        return hash;
    }

    private StructInfo constructStructInfo(GroupExpression groupExpression, List<BitSet> children,
            Plan originPlan, CascadesContext cascadesContext, boolean tableIdMode) {
        // this plan is not origin plan, should record origin plan in struct info
        Plan plan = constructPlan(groupExpression, children, tableIdMode);
        return originPlan == null ? StructInfo.of(plan, cascadesContext)
                : StructInfo.of(plan, originPlan, cascadesContext);
    }

    private Plan constructPlan(GroupExpression groupExpression, List<BitSet> children, boolean tableIdMode) {
        List<Plan> childrenPlan = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            StructInfoMap structInfoMap = groupExpression.child(i).getStructInfoMap();
            BitSet childMap = children.get(i);
            Pair<GroupExpression, List<BitSet>> groupExpressionBitSetPair
                    = structInfoMap.getGroupExpressionWithChildren(childMap, tableIdMode);
            childrenPlan.add(
                    constructPlan(groupExpressionBitSetPair.first, groupExpressionBitSetPair.second, tableIdMode));
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
     */
    public void refresh(Group group, CascadesContext cascadesContext,
            BitSet targetBitSet, Set<Integer> refreshedGroup,
            boolean forceRefresh, int memoVersion, boolean tableIdMode) {
        IdModeStrategy strategy = getStrategy(tableIdMode);
        Map<BitSet, Pair<GroupExpression, List<BitSet>>> groupExprMap = strategy.getGroupExpressionMap(this);
        StructInfoMap structInfoMap = group.getStructInfoMap();
        refreshedGroup.add(group.getGroupId().asInt());
        if (!structInfoMap.getTableMaps(tableIdMode).isEmpty()
                && memoVersion == structInfoMap.getRefreshVersion(targetBitSet)) {
            return;
        }
        for (GroupExpression groupExpression : group.getLogicalExpressions()) {
            List<Set<BitSet>> childrenTableMap = new LinkedList<>();
            if (groupExpression.children().isEmpty()) {
                BitSet leaf = strategy.constructLeaf(groupExpression, cascadesContext, forceRefresh);
                if (leaf.isEmpty()) {
                    break;
                }
                groupExprMap.put(leaf, Pair.of(groupExpression, new LinkedList<>()));
                continue;
            }
            // this is used for filter group expression whose children's table map all not in targetBitSet
            BitSet filteredTableMaps = new BitSet();
            // groupExpression self could be pruned
            for (Group child : groupExpression.children()) {
                // group in expression should all be reserved
                StructInfoMap childStructInfoMap = child.getStructInfoMap();
                if (!refreshedGroup.contains(child.getGroupId().asInt())) {
                    childStructInfoMap.refresh(child, cascadesContext, targetBitSet,
                            refreshedGroup, forceRefresh, memoVersion, tableIdMode);
                    childStructInfoMap.setRefreshVersion(targetBitSet, memoVersion);
                }
                Set<BitSet> groupTableSet = new HashSet<>();
                for (BitSet tableMaps : child.getStructInfoMap().getTableMaps(tableIdMode)) {
                    groupTableSet.add(tableMaps);
                    filteredTableMaps.or(tableMaps);
                }
                if (!filteredTableMaps.isEmpty()) {
                    childrenTableMap.add(groupTableSet);
                }
            }
            // filter the tableSet that used intersects with targetBitSet, make sure the at least constructed
            if (!structInfoMap.getTableMaps(tableIdMode).isEmpty() && !targetBitSet.isEmpty()
                    && !filteredTableMaps.isEmpty() && !filteredTableMaps.intersects(targetBitSet)) {
                continue;
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
            if (groupExprMap.containsKey(eachGroupExpressionTableSet)) {
                // for the group expressions of group, only need to refresh any of the group expression
                // when they have the same group expression table set
                continue;
            }
            // if cumulative child table map is different from current
            // or current group expression map is empty, should update the groupExpressionMap currently
            Collection<Pair<BitSet, List<BitSet>>> bitSetWithChildren = cartesianProduct(childrenTableMap);
            for (Pair<BitSet, List<BitSet>> bitSetWithChild : bitSetWithChildren) {
                groupExprMap.putIfAbsent(bitSetWithChild.first,
                        Pair.of(groupExpression, bitSetWithChild.second));
            }
        }
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

    /**
     * Strategy interface to handle different ID modes (tableId vs relationId)
     */
    private interface IdModeStrategy {
        Map<BitSet, Pair<GroupExpression, List<BitSet>>> getGroupExpressionMap(StructInfoMap structInfoMap);

        Map<BitSet, StructInfo> getInfoMap(StructInfoMap structInfoMap);

        BitSet constructLeaf(GroupExpression groupExpression, CascadesContext cascadesContext, boolean forceRefresh);

        int computeMemoVersion(BitSet targetIdMap, CascadesContext cascadesContext);
    }

    private static IdModeStrategy getStrategy(boolean tableIdMode) {
        return tableIdMode ? TABLE_ID_STRATEGY : RELATION_ID_STRATEGY;
    }

    @Override
    public String toString() {
        return "StructInfoMap{"
                + " groupExpressionMapByRelationId=" + groupExpressionMapByRelationId.keySet()
                + ", infoMapByRelationId=" + infoMapByRelationId.keySet()
                + ", groupExpressionMapByTableId=" + groupExpressionMapByTableId.keySet()
                + ", infoMapByTableId=" + infoMapByTableId.keySet()
                + ", refreshVersion=" + refreshVersion
                + '}';
    }
}
