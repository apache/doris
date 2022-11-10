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

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Context of physical plan.
 */
public class PlanTranslatorContext {
    private final List<PlanFragment> planFragments = Lists.newArrayList();

    private final DescriptorTable descTable = new DescriptorTable();

    private final RuntimeFilterTranslator translator;

    /**
     * index from Nereids' slot to legacy slot.
     */
    private final Map<ExprId, SlotRef> exprIdToSlotRef = Maps.newHashMap();

    /**
     * Inverted index from legacy slot to Nereids' slot.
     */
    private final Map<SlotId, ExprId> slotIdToExprId = Maps.newHashMap();

    private final List<ScanNode> scanNodes = Lists.newArrayList();

    private final IdGenerator<PlanFragmentId> fragmentIdGenerator = PlanFragmentId.createGenerator();

    private final IdGenerator<PlanNodeId> nodeIdGenerator = PlanNodeId.createGenerator();

    public PlanTranslatorContext(CascadesContext ctx) {
        this.translator = new RuntimeFilterTranslator(ctx.getRuntimeFilterContext());
    }

    @VisibleForTesting
    public PlanTranslatorContext() {
        translator = null;
    }

    public List<PlanFragment> getPlanFragments() {
        return planFragments;
    }

    public TupleDescriptor generateTupleDesc() {
        return descTable.createTupleDescriptor();
    }

    public Optional<RuntimeFilterTranslator> getRuntimeTranslator() {
        return Optional.ofNullable(translator);
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

    public void removePlanFragment(PlanFragment planFragment) {
        this.planFragments.remove(planFragment);
    }

    public SlotRef findSlotRef(ExprId exprId) {
        return exprIdToSlotRef.get(exprId);
    }

    public void addScanNode(ScanNode scanNode) {
        scanNodes.add(scanNode);
    }

    public ExprId findExprId(SlotId slotId) {
        return slotIdToExprId.get(slotId);
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    /**
     * Create SlotDesc and add it to the mappings from expression to the stales epxr
     */
    public SlotDescriptor createSlotDesc(TupleDescriptor tupleDesc, SlotReference slotReference) {
        SlotDescriptor slotDescriptor = this.addSlotDesc(tupleDesc);
        Optional<Column> column = slotReference.getColumn();
        // Only the SlotDesc that in the tuple generated for scan node would have corresponding column.
        if (column.isPresent()) {
            slotDescriptor.setColumn(column.get());
        }
        slotDescriptor.setType(slotReference.getDataType().toCatalogDataType());
        slotDescriptor.setIsMaterialized(true);
        SlotRef slotRef = new SlotRef(slotDescriptor);
        slotRef.setLabel(slotReference.getName());
        this.addExprIdSlotRefPair(slotReference.getExprId(), slotRef);
        slotDescriptor.setIsNullable(slotReference.nullable());
        return slotDescriptor;
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
}
