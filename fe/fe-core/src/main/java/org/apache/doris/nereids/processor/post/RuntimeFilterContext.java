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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.RuntimeFilterGenerator.FilterSizeLimits;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * runtime filter context used at post process and translation.
 */
public class RuntimeFilterContext {

    /**
     * the combination of src expr, filter type and source join node, use to identify the filter
      */
    public class RuntimeFilterIdentity {
        final Expression expr;
        final TRuntimeFilterType type;
        final AbstractPhysicalJoin join;

        public RuntimeFilterIdentity(Expression expr, TRuntimeFilterType type, AbstractPhysicalJoin join) {
            this.expr = expr;
            this.type = type;
            this.join = join;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null) {
                return false;
            }
            if (other instanceof RuntimeFilterIdentity) {
                RuntimeFilterIdentity otherId = (RuntimeFilterIdentity) other;
                return expr.equals(otherId.expr) && type.equals(otherId.type) && join.equals(otherId.join);
            }
            return false;
        }
    }

    public List<RuntimeFilter> prunedRF = Lists.newArrayList();

    // exprId of target to runtime filter.
    private final Map<ExprId, List<RuntimeFilter>> targetExprIdToFilter = Maps.newHashMap();

    private final Map<RuntimeFilterIdentity, RuntimeFilter> runtimeFilterIdentityToFilter = Maps.newHashMap();

    private final Map<PlanNodeId, DataStreamSink> planNodeIdToCTEDataSinkMap = Maps.newHashMap();
    private final Map<Plan, List<ExprId>> joinToTargetExprId = Maps.newHashMap();

    // olap scan node that contains target of a runtime filter.
    private final Map<PhysicalRelation, List<Slot>> targetOnOlapScanNodeMap = Maps.newHashMap();

    private final List<org.apache.doris.planner.RuntimeFilter> legacyFilters = Lists.newArrayList();

    // exprId to olap scan node slotRef because the slotRef will be changed when translating.
    private final Map<ExprId, SlotRef> exprIdToOlapScanNodeSlotRef = Maps.newHashMap();

    // alias -> alias's child, if there's a key that is alias's child, the key-value will change by this way
    // Alias(A) = B, now B -> A in map, and encounter Alias(B) -> C, the kv will be C -> A.
    // you can see disjoint set data structure to learn the processing detailed.
    private final Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = Maps.newHashMap();

    private final Map<Slot, ScanNode> scanNodeOfLegacyRuntimeFilterTarget = Maps.newLinkedHashMap();

    private final Map<Plan, EffectiveSrcType> effectiveSrcNodes = Maps.newHashMap();

    private final Map<CTEId, PhysicalCTEProducer> cteProducerMap = Maps.newLinkedHashMap();

    // cte whose runtime filter has been extracted
    private final Set<CTEId> processedCTE = Sets.newHashSet();

    private final SessionVariable sessionVariable;

    private final FilterSizeLimits limits;

    private int targetNullCount = 0;

    private final List<ExpandRF> expandedRF = Lists.newArrayList();

    private final Map<Plan, Set<PhysicalRelation>> relationsUsedByPlan = Maps.newHashMap();

    /**
     * info about expand rf by inner join
     */
    public static class ExpandRF {
        public AbstractPhysicalJoin buildNode;

        public PhysicalRelation srcNode;
        public PhysicalRelation target1;

        public PhysicalRelation target2;

        public EqualPredicate equal;

        public ExpandRF(AbstractPhysicalJoin buildNode, PhysicalRelation srcNode,
                        PhysicalRelation target1, PhysicalRelation target2, EqualPredicate equal) {
            this.buildNode = buildNode;
            this.srcNode = srcNode;
            this.target1 = target1;
            this.target2 = target2;
        }
    }

    public RuntimeFilterContext(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
        this.limits = new FilterSizeLimits(sessionVariable);
    }

    public void setRelationsUsedByPlan(Plan plan, Set<PhysicalRelation> relations) {
        relationsUsedByPlan.put(plan, relations);
    }

    /**
     * return true, if the relation is in the subtree
     */
    public boolean isRelationUseByPlan(Plan plan, PhysicalRelation relation) {
        Set<PhysicalRelation> relations = relationsUsedByPlan.get(plan);
        if (relations == null) {
            relations = Sets.newHashSet();
            RuntimeFilterGenerator.getAllScanInfo(plan, relations);
            relationsUsedByPlan.put(plan, relations);
        }
        return relations.contains(relation);
    }

    public SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public FilterSizeLimits getLimits() {
        return limits;
    }

    public Map<CTEId, PhysicalCTEProducer> getCteProduceMap() {
        return cteProducerMap;
    }

    public Set<CTEId> getProcessedCTE() {
        return processedCTE;
    }

    public void setTargetExprIdToFilter(ExprId id, RuntimeFilter filter) {
        Preconditions.checkArgument(filter.getTargetSlots().stream().anyMatch(expr -> expr.getExprId() == id));
        this.targetExprIdToFilter.computeIfAbsent(id, k -> Lists.newArrayList()).add(filter);
    }

    /**
     * remove the given target from runtime filters from builderNode to target with all runtime filter types
     *
     * @param targetId rf target
     * @param builderNode rf src
     */
    public void removeFilters(ExprId targetId, PhysicalHashJoin builderNode) {
        List<RuntimeFilter> filters = targetExprIdToFilter.get(targetId);
        if (filters != null) {
            Iterator<RuntimeFilter> filterIter = filters.iterator();
            while (filterIter.hasNext()) {
                RuntimeFilter rf = filterIter.next();
                if (rf.getBuilderNode().equals(builderNode)) {
                    Iterator<Slot> targetSlotIter = rf.getTargetSlots().listIterator();
                    Iterator<PhysicalRelation> targetScanIter = rf.getTargetScans().iterator();
                    Iterator<Expression> targetExpressionIter = rf.getTargetExpressions().iterator();
                    Slot targetSlot;
                    PhysicalRelation targetScan;
                    while (targetScanIter.hasNext() && targetSlotIter.hasNext() && targetExpressionIter.hasNext()) {
                        targetExpressionIter.next();
                        targetScan = targetScanIter.next();
                        targetSlot = targetSlotIter.next();
                        if (targetSlot.getExprId().equals(targetId)) {
                            targetScan.removeAppliedRuntimeFilter(rf);
                            targetExpressionIter.remove();
                            targetScanIter.remove();
                            targetSlotIter.remove();
                        }
                    }
                    if (rf.getTargetSlots().isEmpty()) {
                        builderNode.getRuntimeFilters().remove(rf);
                        filterIter.remove();
                        prunedRF.add(rf);
                    }
                }
            }
        }
    }

    /**
     * remove one target from rf, and if there is no target, remove the rf
     */
    public void removeFilter(RuntimeFilter rf, ExprId targetId) {
        Iterator<Slot> targetSlotIter = rf.getTargetSlots().listIterator();
        Iterator<PhysicalRelation> targetScanIter = rf.getTargetScans().iterator();
        Iterator<Expression> targetExpressionIter = rf.getTargetExpressions().iterator();
        Slot targetSlot;
        PhysicalRelation targetScan;
        while (targetScanIter.hasNext() && targetSlotIter.hasNext() && targetExpressionIter.hasNext()) {
            targetExpressionIter.next();
            targetScan = targetScanIter.next();
            targetSlot = targetSlotIter.next();
            if (targetSlot.getExprId().equals(targetId)) {
                targetScan.removeAppliedRuntimeFilter(rf);
                targetExpressionIter.remove();
                targetScanIter.remove();
                targetSlotIter.remove();
            }
        }
        if (rf.getTargetSlots().isEmpty()) {
            rf.getBuilderNode().getRuntimeFilters().remove(rf);
            targetExprIdToFilter.get(targetId).remove(rf);
            prunedRF.add(rf);
        }
    }

    public void setTargetsOnScanNode(PhysicalRelation relation, Slot slot) {
        this.targetOnOlapScanNodeMap.computeIfAbsent(relation, k -> Lists.newArrayList()).add(slot);
    }

    public RuntimeFilter getRuntimeFilterBySrcAndType(Expression src,
                                                      TRuntimeFilterType type, AbstractPhysicalJoin join) {
        return runtimeFilterIdentityToFilter.get(new RuntimeFilterIdentity(src, type, join));
    }

    public void setRuntimeFilterIdentityToFilter(Expression src, TRuntimeFilterType type,
                                                 AbstractPhysicalJoin join, RuntimeFilter rf) {
        runtimeFilterIdentityToFilter.put(new RuntimeFilterIdentity(src, type, join), rf);
    }

    public Map<ExprId, SlotRef> getExprIdToOlapScanNodeSlotRef() {
        return exprIdToOlapScanNodeSlotRef;
    }

    public Map<PlanNodeId, DataStreamSink> getPlanNodeIdToCTEDataSinkMap() {
        return planNodeIdToCTEDataSinkMap;
    }

    public Map<NamedExpression, Pair<PhysicalRelation, Slot>> getAliasTransferMap() {
        return aliasTransferMap;
    }

    public Pair<PhysicalRelation, Slot> getAliasTransferPair(NamedExpression slot) {
        return aliasTransferMap.get(slot);
    }

    public Pair<PhysicalRelation, Slot> aliasTransferMapPut(NamedExpression slot, Pair<PhysicalRelation, Slot> pair) {
        return aliasTransferMap.put(slot, pair);
    }

    public boolean aliasTransferMapContains(NamedExpression slot) {
        return aliasTransferMap.containsKey(slot);
    }

    public Map<Slot, ScanNode> getScanNodeOfLegacyRuntimeFilterTarget() {
        return scanNodeOfLegacyRuntimeFilterTarget;
    }

    public Map<ExprId, List<RuntimeFilter>> getTargetExprIdToFilter() {
        return targetExprIdToFilter;
    }

    public List<Slot> getTargetListByScan(PhysicalRelation scan) {
        return targetOnOlapScanNodeMap.getOrDefault(scan, Collections.emptyList());
    }

    public List<org.apache.doris.planner.RuntimeFilter> getLegacyFilters() {
        return legacyFilters;
    }

    /**
     * get nereids runtime filters
     * @return nereids runtime filters
     */
    @VisibleForTesting
    public List<RuntimeFilter> getNereidsRuntimeFilter() {
        List<RuntimeFilter> filters = getTargetExprIdToFilter().values().stream()
                .reduce(Lists.newArrayList(), (l, r) -> {
                    l.addAll(r);
                    return l;
                });
        filters = filters.stream().distinct().collect(Collectors.toList());
        filters.sort((a, b) -> a.getId().compareTo(b.getId()));
        return filters;
    }

    public void setTargetNullCount() {
        targetNullCount++;
    }

    /**
     * the selectivity produced by predicate or rf
     */
    public enum EffectiveSrcType {
        NATIVE, REF
    }

    public void addEffectiveSrcNode(Plan node, EffectiveSrcType type) {
        effectiveSrcNodes.put(node, type);
    }

    public boolean isEffectiveSrcNode(Plan node) {
        return effectiveSrcNodes.keySet().contains(node);
    }

    public EffectiveSrcType getEffectiveSrcType(Plan plan) {
        return effectiveSrcNodes.get(plan);
    }

    @VisibleForTesting
    public int getTargetNullCount() {
        return targetNullCount;
    }

    public void addJoinToTargetMap(AbstractPhysicalJoin join, ExprId exprId) {
        joinToTargetExprId.computeIfAbsent(join, k -> Lists.newArrayList()).add(exprId);
    }

    public List<ExprId> getTargetExprIdByFilterJoin(AbstractPhysicalJoin join) {
        return joinToTargetExprId.getOrDefault(join, Lists.newArrayList());
    }

    /**
     * return the info about expand_runtime_filter_by_inner_join
     */
    public ExpandRF getExpandRfByJoin(AbstractPhysicalJoin join) {
        if (join instanceof PhysicalHashJoin) {
            for (ExpandRF expand : expandedRF) {
                if (expand.buildNode.equals(join)) {
                    return expand;
                }
            }
        }
        return null;
    }

    public List<ExpandRF> getExpandedRF() {
        return expandedRF;
    }
}
