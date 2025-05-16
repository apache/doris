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
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Representation for group in cascades optimizer.
 */
public class StructInfoMap {
    private static final ReserveNecessaryNode RESERVE_NECESSARY_NODE = new ReserveNecessaryNode();
    private final Multimap<BitSet, Pair<GroupExpression, List<BitSet>>> groupExpressionMap = HashMultimap.create();
    private final Multimap<BitSet, StructInfo> infoMap = HashMultimap.create();
    private long refreshVersion = 0;

    /**
     * get struct info according to table map
     *
     * @param tableMap the original table map
     * @param group the group that the mv matched
     * @return struct info or null if not found
     */
    public Collection<StructInfo> getStructInfo(CascadesContext cascadesContext, BitSet tableMap, Group group,
            Plan originPlan) {
        Collection<StructInfo> structInfo = infoMap.get(tableMap);
        if (!structInfo.isEmpty()) {
            return structInfo;
        } else {
            structInfo = new ArrayList<>();
        }
        if (groupExpressionMap.isEmpty() || !groupExpressionMap.containsKey(tableMap)) {
            refresh(group, cascadesContext, new HashSet<>());
            group.getstructInfoMap().setRefreshVersion(cascadesContext.getMemo().getRefreshVersion());
        }
        if (groupExpressionMap.containsKey(tableMap)) {
            Collection<Pair<GroupExpression, List<BitSet>>> groupExpressionBitSetPair =
                    getGroupExpressionWithChildren(tableMap);
            for (Pair<GroupExpression, List<BitSet>> each : groupExpressionBitSetPair) {
                structInfo.addAll(constructStructInfo(
                        each.first, each.second, tableMap, originPlan, cascadesContext));
            }
            infoMap.putAll(tableMap, structInfo);
        }
        return structInfo;
    }

    public Set<BitSet> getTableMaps() {
        return groupExpressionMap.keySet();
    }

    public Collection<StructInfo> getStructInfos() {
        return infoMap.values();
    }

    public Collection<Pair<GroupExpression, List<BitSet>>> getGroupExpressionWithChildren(BitSet tableMap) {
        return groupExpressionMap.get(tableMap);
    }

    public void setRefreshVersion(long refreshVersion) {
        this.refreshVersion = refreshVersion;
    }

    private Collection<StructInfo> constructStructInfo(GroupExpression groupExpression, List<BitSet> children,
            BitSet tableMap, Plan originPlan, CascadesContext cascadesContext) {
        // this plan is not origin plan, should record origin plan in struct info
        List<Plan> plan = constructPlan(groupExpression, children, tableMap);
        List<StructInfo> results = new ArrayList<>();
        plan.forEach(each -> results.add(StructInfo.of(each, originPlan, cascadesContext)));
        return results;
    }

    private List<Plan> constructPlan(GroupExpression groupExpression, List<BitSet> children, BitSet tableMap) {
        List<Plan> results = new ArrayList<>();
        List<List<Plan>> childrenPlan = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            StructInfoMap structInfoMap = groupExpression.child(i).getstructInfoMap();
            BitSet childMap = children.get(i);
            Collection<Pair<GroupExpression, List<BitSet>>> groupExpressionBitSetPair
                    = structInfoMap.getGroupExpressionWithChildren(childMap);

            List<Plan> childPlans = new ArrayList<>();
            groupExpressionBitSetPair.forEach(each ->
                    childPlans.addAll(constructPlan(each.first, each.second, childMap))
            );
            childrenPlan.add(childPlans);
        }
        // Notice expand too large
        List<List<Plan>> childrenCmp = Lists.cartesianProduct(childrenPlan);
        childrenCmp.forEach(eachCmp -> results.add(groupExpression.getPlan().withChildren(eachCmp)));
        return results;
    }

    /**
     * refresh group expression map
     *
     * @param group the root group
     *
     */
    public void refresh(Group group, CascadesContext cascadesContext, Set<Integer> refreshedGroup) {
        StructInfoMap structInfoMap = group.getstructInfoMap();
        refreshedGroup.add(group.getGroupId().asInt());
        long memoVersion = cascadesContext.getMemo().getRefreshVersion();
        if (!structInfoMap.getTableMaps().isEmpty() && memoVersion == structInfoMap.refreshVersion) {
            return;
        }
        Multimap<BitSet, Plan> groupExpressionRefreshedMap = HashMultimap.create();
        outer:
        for (GroupExpression groupExpression : group.getLogicalExpressions()) {
            // Record each group bit set, Set<BitSet> is belonged to one group
            List<Set<BitSet>> childrenGroupTableMap = new LinkedList<>();
            if (groupExpression.children().isEmpty()) {
                BitSet leaf = constructLeaf(groupExpression, cascadesContext);
                if (leaf.isEmpty()) {
                    break;
                }
                groupExpressionMap.put(leaf, Pair.of(groupExpression, new LinkedList<>()));
                continue;
            }
            for (Group child : groupExpression.children()) {
                StructInfoMap childStructInfoMap = child.getstructInfoMap();
                if (!refreshedGroup.contains(child.getGroupId().asInt())) {
                    childStructInfoMap.refresh(child, cascadesContext, refreshedGroup);
                    childStructInfoMap.setRefreshVersion(memoVersion);
                }
                childrenGroupTableMap.add(child.getstructInfoMap().getTableMaps());
            }
            // if one same groupExpression have refreshed, continue
            BitSet groupExpressionTableSet = new BitSet();
            for (Set<BitSet> groupExpressionBitSet : childrenGroupTableMap) {
                for (BitSet each : groupExpressionBitSet) {
                    groupExpressionTableSet.or(each);
                }
            }
            // if not equals by logical, need to add to groupExpressionMap
            Plan currentGroupExpressionPlan = groupExpression.getPlan();
            if (groupExpressionRefreshedMap.keySet().contains(groupExpressionTableSet)) {
                Collection<Plan> existPlans = groupExpressionRefreshedMap.get(groupExpressionTableSet);
                boolean equals;
                for (Plan existPlan : existPlans) {
                    equals = isStructInfoLogicalEquals(currentGroupExpressionPlan, existPlan);
                    if (equals) {
                        continue outer;
                    }
                }
                continue;
            } else {
                groupExpressionRefreshedMap.put(groupExpressionTableSet, currentGroupExpressionPlan);
            }
            // if cumulative child table map is different from current
            // or current group expression map is empty, should update the groupExpressionMap currently
            Collection<Pair<BitSet, List<BitSet>>> bitSetWithChildren = cartesianProduct(childrenGroupTableMap);
            for (Pair<BitSet, List<BitSet>> bitSetWithChild : bitSetWithChildren) {
                groupExpressionMap.put(bitSetWithChild.first, Pair.of(groupExpression, bitSetWithChild.second));
            }
        }
    }

    private BitSet constructLeaf(GroupExpression groupExpression, CascadesContext cascadesContext) {
        Plan plan = groupExpression.getPlan();
        BitSet tableMap = new BitSet();
        boolean enableMaterializedViewNestRewrite = cascadesContext.getConnectContext().getSessionVariable()
                .isEnableMaterializedViewNestRewrite();
        if (plan instanceof LogicalCatalogRelation) {
            TableIf table = ((LogicalCatalogRelation) plan).getTable();
            // If disable materialized view nest rewrite, and mv already rewritten successfully once, doesn't construct
            // table id map for nest mv rewrite
            if (!enableMaterializedViewNestRewrite
                    && cascadesContext.getMaterializationRewrittenSuccessSet().contains(table.getFullQualifiers())) {
                return tableMap;

            }
            tableMap.set(cascadesContext.getStatementContext()
                    .getTableId(table).asInt());
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

    public boolean isStructInfoLogicalEquals(Plan source, Plan target) {
        source = source.accept(RESERVE_NECESSARY_NODE, null);
        target = target.accept(RESERVE_NECESSARY_NODE, null);
        return source.getClass().equals(target.getClass());
    }

    private static class ReserveNecessaryNode extends DefaultPlanRewriter<Void> {
        @Override
        public Plan visit(Plan plan, Void context) {
            if (plan instanceof LogicalProject || plan instanceof LogicalFilter) {
                return super.visit(plan, context);
            }
            return plan;
        }
    }
}
