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
import org.apache.doris.catalog.Column;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Context of physical plan.
 */
public class PlanTranslatorContext {
    private final List<PlanFragment> planFragments = Lists.newArrayList();

    private final DescriptorTable descTable = new DescriptorTable();

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

    public List<PlanFragment> getPlanFragments() {
        return planFragments;
    }

    public TupleDescriptor generateTupleDesc() {
        return descTable.createTupleDescriptor();
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
        Column column = slotReference.getColumn();
        // Only the SlotDesc that in the tuple generated for scan node would have corresponding column.
        if (column != null) {
            slotDescriptor.setColumn(column);
        }
        slotDescriptor.setType(slotReference.getDataType().toCatalogDataType());
        slotDescriptor.setIsMaterialized(true);
        this.addExprIdSlotRefPair(slotReference.getExprId(), new SlotRef(slotDescriptor));
        return slotDescriptor;
    }

    /**
     * Create slotDesc with Expression.
     */
    public void createSlotDesc(TupleDescriptor tupleDesc, Expression expression) {
        SlotDescriptor slotDescriptor = this.addSlotDesc(tupleDesc);
        slotDescriptor.setType(expression.getDataType().toCatalogDataType());
    }

    /**
     * in Nereids, all node only has one TupleDescriptor, so we can use the first one.
     *
     * @param planNode the node to get the TupleDescriptor
     *
     * @return plan node's tuple descriptor
     */
    public TupleDescriptor getTupleDesc(PlanNode planNode) {
        return descTable.getTupleDesc(planNode.getOutputTupleIds().get(0));
    }

    public DescriptorTable getDescTable() {
        return descTable;
    }
}
