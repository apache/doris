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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.ColumnRefExpr;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.analysis.VirtualSlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.TopnFilterContext;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.planner.CTEScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Context of physical plan.
 */
public class PlanTranslatorContext {
    private final ConnectContext connectContext;
    private final List<PlanFragment> planFragments = Lists.newArrayList();

    private final DescriptorTable descTable = new DescriptorTable();

    private final RuntimeFilterTranslator translator;
    private final TopnFilterContext topnFilterContext;
    /**
     * index from Nereids' slot to legacy slot.
     */
    private final Map<ExprId, SlotRef> exprIdToSlotRef = Maps.newHashMap();

    /**
     * Inverted index from legacy slot to Nereids' slot.
     */
    private final Map<SlotId, ExprId> slotIdToExprId = Maps.newHashMap();

    /**
     * For each lambda argument (ArrayItemReference),
     * we create a ColumnRef representing it and
     * then translate it based on the ExprId of the ArrayItemReference.
     */
    private final Map<ExprId, ColumnRefExpr> exprIdToColumnRef = Maps.newHashMap();

    private final List<ScanNode> scanNodes = Lists.newArrayList();
    private final List<PhysicalRelation> physicalRelations = Lists.newArrayList();
    private final IdGenerator<PlanFragmentId> fragmentIdGenerator = PlanFragmentId.createGenerator();

    private final IdGenerator<PlanNodeId> nodeIdGenerator = PlanNodeId.createGenerator();

    private final IdentityHashMap<PlanFragment, PhysicalHashAggregate> firstAggInFragment
            = new IdentityHashMap<>();

    private final Map<ExprId, SlotRef> bufferedSlotRefForWindow = Maps.newHashMap();
    private TupleDescriptor bufferedTupleForWindow = null;

    private final Map<CTEId, PlanFragment> cteProduceFragments = Maps.newHashMap();

    private final Map<CTEId, PhysicalCTEProducer> cteProducerMap = Maps.newHashMap();

    private final Map<CTEId, PhysicalCTEConsumer> cteConsumerMap = Maps.newHashMap();

    private final Map<PlanFragmentId, CTEScanNode> cteScanNodeMap = Maps.newHashMap();

    private final Map<RelationId, TPushAggOp> tablePushAggOp = Maps.newHashMap();

    private final Map<ScanNode, Set<SlotId>> statsUnknownColumnsMap = Maps.newHashMap();

    public PlanTranslatorContext(CascadesContext ctx) {
        this.connectContext = ctx.getConnectContext();
        this.translator = new RuntimeFilterTranslator(ctx.getRuntimeFilterContext());
        this.topnFilterContext = ctx.getTopnFilterContext();
    }

    @VisibleForTesting
    public PlanTranslatorContext() {
        this.connectContext = null;
        this.translator = null;
        this.topnFilterContext = new TopnFilterContext();
    }

    /**
     * remember the unknown-stats column and its scan, used for forbid_unknown_col_stats check
     */
    public void addUnknownStatsColumn(ScanNode scan, SlotId slotId) {
        Set<SlotId> slots = statsUnknownColumnsMap.get(scan);
        if (slots == null) {
            statsUnknownColumnsMap.put(scan, Sets.newHashSet(slotId));
        } else {
            statsUnknownColumnsMap.get(scan).add(slotId);
        }
    }

    public boolean isColumnStatsUnknown(ScanNode scan, SlotId slotId) {
        Set<SlotId> unknownSlots = statsUnknownColumnsMap.get(scan);
        if (unknownSlots == null) {
            return false;
        }
        return unknownSlots.contains(slotId);
    }

    public void removeScanFromStatsUnknownColumnsMap(ScanNode scan) {
        statsUnknownColumnsMap.remove(scan);
    }

    public SessionVariable getSessionVariable() {
        return connectContext == null ? null : connectContext.getSessionVariable();
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public Set<ScanNode> getScanNodeWithUnknownColumnStats() {
        return statsUnknownColumnsMap.keySet();
    }

    public List<PlanFragment> getPlanFragments() {
        return planFragments;
    }

    public Map<CTEId, PlanFragment> getCteProduceFragments() {
        return cteProduceFragments;
    }

    public Map<CTEId, PhysicalCTEProducer> getCteProduceMap() {
        return cteProducerMap;
    }

    public Map<CTEId, PhysicalCTEConsumer> getCteConsumerMap() {
        return cteConsumerMap;
    }

    public Map<PlanFragmentId, CTEScanNode> getCteScanNodeMap() {
        return cteScanNodeMap;
    }

    public TupleDescriptor generateTupleDesc() {
        return descTable.createTupleDescriptor();
    }

    public Optional<RuntimeFilterTranslator> getRuntimeTranslator() {
        return Optional.ofNullable(translator);
    }

    public TopnFilterContext getTopnFilterContext() {
        return topnFilterContext;
    }

    public PlanFragmentId nextFragmentId() {
        return fragmentIdGenerator.getNextId();
    }

    public PlanNodeId nextPlanNodeId() {
        return nodeIdGenerator.getNextId();
    }

    public SlotDescriptor addSlotDesc(TupleDescriptor t) {
        return descTable.addSlotDescriptor(t);
    }

    public void addPlanFragment(PlanFragment planFragment) {
        this.planFragments.add(planFragment);
    }

    public void addExprIdSlotRefPair(ExprId exprId, SlotRef slotRef) {
        exprIdToSlotRef.put(exprId, slotRef);
        slotIdToExprId.put(slotRef.getDesc().getId(), exprId);
    }

    public void addExprIdColumnRefPair(ExprId exprId, ColumnRefExpr columnRefExpr) {
        exprIdToColumnRef.put(exprId, columnRefExpr);
    }

    /**
     * merge source fragment info into target fragment.
     * include runtime filter info and fragment attribute.
     */
    public void mergePlanFragment(PlanFragment srcFragment, PlanFragment targetFragment) {
        srcFragment.getTargetRuntimeFilterIds().forEach(targetFragment::setTargetRuntimeFilterIds);
        srcFragment.getBuilderRuntimeFilterIds().forEach(targetFragment::setBuilderRuntimeFilterIds);
        targetFragment.setHasColocatePlanNode(targetFragment.hasColocatePlanNode()
                || srcFragment.hasColocatePlanNode());
        this.planFragments.remove(srcFragment);
    }

    public SlotRef findSlotRef(ExprId exprId) {
        return exprIdToSlotRef.get(exprId);
    }

    public ColumnRefExpr findColumnRef(ExprId exprId) {
        return exprIdToColumnRef.get(exprId);
    }

    public void addScanNode(ScanNode scanNode, PhysicalRelation physicalRelation) {
        scanNodes.add(scanNode);
        physicalRelations.add(physicalRelation);
    }

    public List<PhysicalRelation> getPhysicalRelations() {
        return physicalRelations;
    }

    public ExprId findExprId(SlotId slotId) {
        return slotIdToExprId.get(slotId);
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    public PhysicalHashAggregate getFirstAggregateInFragment(PlanFragment planFragment) {
        return firstAggInFragment.get(planFragment);
    }

    public void setFirstAggregateInFragment(PlanFragment planFragment, PhysicalHashAggregate aggregate) {
        firstAggInFragment.put(planFragment, aggregate);
    }

    public Map<ExprId, SlotRef> getBufferedSlotRefForWindow() {
        return bufferedSlotRefForWindow;
    }

    public TupleDescriptor getBufferedTupleForWindow() {
        return bufferedTupleForWindow;
    }

    /**
     * Create SlotDesc and add it to the mappings from expression to the stales expr.
     */
    public SlotDescriptor createSlotDesc(TupleDescriptor tupleDesc, SlotReference slotReference,
            @Nullable TableIf table) {
        SlotDescriptor slotDescriptor = this.addSlotDesc(tupleDesc);
        // Only the SlotDesc that in the tuple generated for scan node would have corresponding column.
        Optional<Column> column = slotReference.getColumn();
        column.ifPresent(slotDescriptor::setColumn);
        slotDescriptor.setType(slotReference.getDataType().toCatalogDataType());
        slotDescriptor.setIsMaterialized(true);
        SlotRef slotRef;
        if (slotReference instanceof VirtualSlotReference) {
            slotRef = new VirtualSlotRef(slotDescriptor);
            VirtualSlotReference virtualSlot = (VirtualSlotReference) slotReference;
            slotDescriptor.setColumn(new Column(
                    virtualSlot.getName(), virtualSlot.getDataType().toCatalogDataType()));
            slotDescriptor.setLabel(slotReference.getName());
        } else {
            slotRef = new SlotRef(slotDescriptor);
            if (slotReference.hasSubColPath() && slotReference.getColumn().isPresent()) {
                slotDescriptor.setSubColLables(slotReference.getSubPath());
                // use lower case name for variant's root, since backend treat parent column as lower case
                // see issue: https://github.com/apache/doris/pull/32999/commits
                slotDescriptor.setMaterializedColumnName(slotRef.getColumnName().toLowerCase()
                            + "." + String.join(".", slotReference.getSubPath()));
            }
        }
        slotRef.setTable(table);
        slotRef.setLabel(slotReference.getName());
        this.addExprIdSlotRefPair(slotReference.getExprId(), slotRef);
        slotDescriptor.setIsNullable(slotReference.nullable());
        return slotDescriptor;
    }

    public SlotDescriptor createSlotDesc(TupleDescriptor tupleDesc, SlotReference slotReference) {
        return createSlotDesc(tupleDesc, slotReference, null);
    }

    public List<TupleDescriptor> getTupleDesc(PlanNode planNode) {
        if (planNode.getOutputTupleDesc() != null) {
            return Lists.newArrayList(planNode.getOutputTupleDesc());
        }
        return planNode.getOutputTupleIds().stream().map(this::getTupleDesc).collect(Collectors.toList());
    }

    public TupleDescriptor getTupleDesc(TupleId tupleId) {
        return descTable.getTupleDesc(tupleId);
    }

    public DescriptorTable getDescTable() {
        return descTable;
    }

    public void setRelationPushAggOp(RelationId relationId, TPushAggOp aggOp) {
        tablePushAggOp.put(relationId, aggOp);
    }

    public TPushAggOp getRelationPushAggOp(RelationId relationId) {
        return tablePushAggOp.getOrDefault(relationId, TPushAggOp.NONE);
    }
}
