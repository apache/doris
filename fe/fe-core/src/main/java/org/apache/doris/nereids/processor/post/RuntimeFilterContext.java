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
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.RuntimeFilter.FilterSizeLimits;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * runtime filter context used at post process and translation.
 */
public class RuntimeFilterContext {
    public List<RuntimeFilter> prunedRF = Lists.newArrayList();

    // exprId of target to runtime filter.
    private final Map<ExprId, List<RuntimeFilter>> targetExprIdToFilter = Maps.newHashMap();

    private final Map<PlanNodeId, DataStreamSink> planNodeIdToCTEDataSinkMap = Maps.newHashMap();
    private final Map<Plan, List<ExprId>> joinToTargetExprId = Maps.newHashMap();

    // olap scan node that contains target of a runtime filter.
    private final Map<PhysicalRelation, List<Slot>> targetOnOlapScanNodeMap = Maps.newHashMap();

    private final List<org.apache.doris.planner.RuntimeFilter> legacyFilters = Lists.newArrayList();

    // exprId to olap scan node slotRef because the slotRef will be changed when translating.
    private final Map<ExprId, SlotRef> exprIdToOlapScanNodeSlotRef = Maps.newHashMap();

    private final Map<Slot, ScanNode> scanNodeOfLegacyRuntimeFilterTarget = Maps.newLinkedHashMap();

    private final Map<Plan, EffectiveSrcType> effectiveSrcNodes = Maps.newHashMap();

    private final SessionVariable sessionVariable;

    private final FilterSizeLimits limits;

    private int targetNullCount = 0;

    private final IdGenerator<RuntimeFilterId> runtimeFilterIdGen;

    public RuntimeFilterContext(SessionVariable sessionVariable, IdGenerator<RuntimeFilterId> runtimeFilterIdGen) {
        this.sessionVariable = sessionVariable;
        this.limits = new FilterSizeLimits(sessionVariable);
        this.runtimeFilterIdGen = runtimeFilterIdGen;
    }

    public SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public FilterSizeLimits getLimits() {
        return limits;
    }

    public void setTargetExprIdToFilter(ExprId id, RuntimeFilter filter) {
        Preconditions.checkArgument(filter.getTargetSlot().getExprId() == id);
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
                if (rf.getBuilderNode().equals(builderNode) && rf.getTargetSlot().getExprId().equals(targetId)) {
                    rf.getTargetScan().removeAppliedRuntimeFilter(rf);
                    builderNode.getRuntimeFilters().remove(rf);
                    filterIter.remove();
                    prunedRF.add(rf);
                }
            }
        }
    }

    /**
     * remove one target from rf, and if there is no target, remove the rf
     */
    public void removeFilter(RuntimeFilter rf, ExprId targetId) {
        if (!rf.getTargetSlot().getExprId().equals(targetId)) {
            return;
        }
        rf.getTargetScan().removeAppliedRuntimeFilter(rf);
        rf.getBuilderNode().getRuntimeFilters().remove(rf);
        List<RuntimeFilter> filters = targetExprIdToFilter.get(targetId);
        if (filters != null) {
            targetExprIdToFilter.get(targetId).remove(rf);
        }
        prunedRF.add(rf);
    }

    public void setTargetsOnScanNode(PhysicalRelation relation, Slot slot) {
        this.targetOnOlapScanNodeMap.computeIfAbsent(relation, k -> Lists.newArrayList()).add(slot);
    }

    public Map<ExprId, SlotRef> getExprIdToOlapScanNodeSlotRef() {
        return exprIdToOlapScanNodeSlotRef;
    }

    public Map<PlanNodeId, DataStreamSink> getPlanNodeIdToCTEDataSinkMap() {
        return planNodeIdToCTEDataSinkMap;
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

    public void addJoinToTargetMap(AbstractPhysicalPlan join, ExprId exprId) {
        joinToTargetExprId.computeIfAbsent(join, k -> Lists.newArrayList()).add(exprId);
    }

    public List<ExprId> getTargetExprIdByFilterJoin(AbstractPhysicalPlan join) {
        return joinToTargetExprId.getOrDefault(join, Lists.newArrayList());
    }

    public IdGenerator<RuntimeFilterId> getRuntimeFilterIdGen() {
        return runtimeFilterIdGen;
    }
}
