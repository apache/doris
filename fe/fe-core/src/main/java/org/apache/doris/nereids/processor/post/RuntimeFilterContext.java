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
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.RuntimeFilterGenerator.FilterSizeLimits;
import org.apache.doris.planner.RuntimeFilterId;
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

    private final IdGenerator<RuntimeFilterId> generator = RuntimeFilterId.createGenerator();

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

    private final Map<AbstractPhysicalJoin, Set<RuntimeFilter>> runtimeFilterOnHashJoinNode = Maps.newHashMap();

    // alias -> alias's child, if there's a key that is alias's child, the key-value will change by this way
    // Alias(A) = B, now B -> A in map, and encounter Alias(B) -> C, the kv will be C -> A.
    // you can see disjoint set data structure to learn the processing detailed.
    private final Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = Maps.newHashMap();

    private final Map<Slot, ScanNode> scanNodeOfLegacyRuntimeFilterTarget = Maps.newLinkedHashMap();

    private final Set<Plan> effectiveSrcNodes = Sets.newHashSet();

    // cte to related joins map which can extract common runtime filter to cte inside
    private final Map<CTEId, Set<PhysicalHashJoin>> cteToJoinsMap = Maps.newLinkedHashMap();

    // cte candidates which can be pushed into common runtime filter into from outside
    private final Map<PhysicalCTEProducer, Map<EqualTo, PhysicalHashJoin>> cteRFPushDownMap = Maps.newLinkedHashMap();

    private final Map<CTEId, PhysicalCTEProducer> cteProducerMap = Maps.newLinkedHashMap();

    // cte whose runtime filter has been extracted
    private final Set<CTEId> processedCTE = Sets.newHashSet();

    // cte whose outer runtime filter has been pushed down into
    private final Set<CTEId> pushedDownCTE = Sets.newHashSet();

    private final SessionVariable sessionVariable;

    private final FilterSizeLimits limits;

    private int targetNullCount = 0;

    public RuntimeFilterContext(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
        this.limits = new FilterSizeLimits(sessionVariable);
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

    public Map<PhysicalCTEProducer, Map<EqualTo, PhysicalHashJoin>> getCteRFPushDownMap() {
        return cteRFPushDownMap;
    }

    public Map<CTEId, Set<PhysicalHashJoin>> getCteToJoinsMap() {
        return cteToJoinsMap;
    }

    public Set<CTEId> getProcessedCTE() {
        return processedCTE;
    }

    public Set<CTEId> getPushedDownCTE() {
        return pushedDownCTE;
    }

    public void setTargetExprIdToFilter(ExprId id, RuntimeFilter filter) {
        Preconditions.checkArgument(filter.getTargetExprs().stream().anyMatch(expr -> expr.getExprId() == id));
        this.targetExprIdToFilter.computeIfAbsent(id, k -> Lists.newArrayList()).add(filter);
    }

    /**
     * remove rf from builderNode to target
     *
     * @param targetId rf target
     * @param builderNode rf src
     */
    public void removeFilter(ExprId targetId, PhysicalHashJoin builderNode) {
        List<RuntimeFilter> filters = targetExprIdToFilter.get(targetId);
        if (filters != null) {
            Iterator<RuntimeFilter> iter = filters.iterator();
            while (iter.hasNext()) {
                RuntimeFilter rf = iter.next();
                if (rf.getBuilderNode().equals(builderNode)) {
                    builderNode.getRuntimeFilters().remove(rf);
                    for (Slot target : rf.getTargetSlots()) {
                        if (target.getExprId().equals(targetId)) {
                            Pair<PhysicalRelation, Slot> pair = aliasTransferMap.get(target);
                            if (pair != null) {
                                pair.first.removeAppliedRuntimeFilter(rf);
                            }
                        }
                    }
                    iter.remove();
                    prunedRF.add(rf);
                }
            }
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

    public Map<Slot, ScanNode> getScanNodeOfLegacyRuntimeFilterTarget() {
        return scanNodeOfLegacyRuntimeFilterTarget;
    }

    public Set<RuntimeFilter> getRuntimeFilterOnHashJoinNode(AbstractPhysicalJoin join) {
        return runtimeFilterOnHashJoinNode.getOrDefault(join, Collections.emptySet());
    }

    public void generatePhysicalHashJoinToRuntimeFilter() {
        targetExprIdToFilter.values().forEach(filters -> filters.forEach(filter -> runtimeFilterOnHashJoinNode
                .computeIfAbsent(filter.getBuilderNode(), k -> Sets.newHashSet()).add(filter)));
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

    public void addEffectiveSrcNode(Plan node) {
        effectiveSrcNodes.add(node);
    }

    public boolean isEffectiveSrcNode(Plan node) {
        return effectiveSrcNodes.contains(node);
    }

    @VisibleForTesting
    public int getTargetNullCount() {
        return targetNullCount;
    }

    public void addJoinToTargetMap(AbstractPhysicalJoin join, ExprId exprId) {
        joinToTargetExprId.computeIfAbsent(join, k -> Lists.newArrayList()).add(exprId);
    }

    public List<ExprId> getTargetExprIdByFilterJoin(AbstractPhysicalJoin join) {
        return joinToTargetExprId.get(join);
    }

    public SlotReference getCorrespondingOlapSlotReference(SlotReference slot) {
        SlotReference olapSlot = slot;
        if (aliasTransferMap.containsKey(olapSlot)) {
            olapSlot = (SlotReference) aliasTransferMap.get(olapSlot).second;
        }
        return olapSlot;
    }
}
