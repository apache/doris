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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.GroupByClause;
import org.apache.doris.analysis.GroupingFunctionCallExpr;
import org.apache.doris.analysis.GroupingInfo;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.analysis.VirtualSlotRef;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TRepeatNode;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used for grouping sets.
 * It will add some new rows and a column of groupingId according to grouping sets info.
 */
public class RepeatNode extends PlanNode {

    private static final Logger LOG = LogManager.getLogger(RepeatNode.class);

    private List<Set<Integer>> repeatSlotIdList;
    private Set<Integer> allSlotId;
    private TupleDescriptor outputTupleDesc;
    private List<List<Long>> groupingList;
    private GroupingInfo groupingInfo;
    private PlanNode input;
    private GroupByClause groupByClause;

    protected RepeatNode(PlanNodeId id, PlanNode input, GroupingInfo groupingInfo, GroupByClause groupByClause) {
        super(id, input.getTupleIds(), "REPEAT_NODE");
        this.children.add(input);
        this.groupingInfo = groupingInfo;
        this.input = input;
        this.groupByClause = groupByClause;

    }

    // only for unittest
    protected RepeatNode(PlanNodeId id, PlanNode input, List<Set<SlotId>> repeatSlotIdList,
                      TupleDescriptor outputTupleDesc, List<List<Long>> groupingList) {
        super(id, input.getTupleIds(), "REPEAT_NODE");
        this.children.add(input);
        this.repeatSlotIdList = buildIdSetList(repeatSlotIdList);
        this.groupingList = groupingList;
        this.outputTupleDesc = outputTupleDesc;
        tupleIds.add(outputTupleDesc.getId());
    }

    private static List<Set<Integer>> buildIdSetList(List<Set<SlotId>> repeatSlotIdList) {
        List<Set<Integer>> slotIdList = new ArrayList<>();
        for (Set slotSet : repeatSlotIdList) {
            Set<Integer> intSet = new HashSet<>();
            for (Object slotId : slotSet) {
                intSet.add(((SlotId) slotId).asInt());
            }
            slotIdList.add(intSet);
        }

        return slotIdList;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        avgRowSize = 0;
        numNodes = 1;
        cardinality = 0;
        if (LOG.isDebugEnabled()) {
            LOG.debug("stats Sort: cardinality=" + cardinality);
        }
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        Preconditions.checkState(conjuncts.isEmpty());
        groupByClause.substituteGroupingExprs(groupingInfo.getGroupingSlots(), input.getOutputSmap(),
                analyzer);

        for (Expr expr : groupByClause.getGroupingExprs()) {
            if (expr instanceof SlotRef || (expr instanceof GroupingFunctionCallExpr)) {
                continue;
            }
            // throw new AnalysisException("function or expr is not allowed in grouping sets clause.");
        }

        // build new BitSet List for tupleDesc
        Set<SlotDescriptor> slotDescSet = new HashSet<>();
        for (TupleId tupleId : input.getTupleIds()) {
            TupleDescriptor tupleDescriptor = analyzer.getDescTbl().getTupleDesc(tupleId);
            slotDescSet.addAll(tupleDescriptor.getSlots());
        }

        // build tupleDesc according to child's tupleDesc info
        outputTupleDesc = groupingInfo.getVirtualTuple();
        //set aggregate nullable
        for (Expr slot : groupByClause.getGroupingExprs()) {
            if (slot instanceof SlotRef && !(slot instanceof VirtualSlotRef)) {
                ((SlotRef) slot).getDesc().setIsNullable(true);
            }
        }
        outputTupleDesc.computeStatAndMemLayout();

        List<Set<SlotId>> groupingIdList = new ArrayList<>();
        List<Expr> exprList = groupByClause.getGroupingExprs();
        Preconditions.checkState(exprList.size() >= 2);
        allSlotId = new HashSet<>();
        for (BitSet bitSet : Collections.unmodifiableList(groupingInfo.getGroupingIdList())) {
            Set<SlotId> slotIdSet = new HashSet<>();
            for (SlotDescriptor slotDesc : slotDescSet) {
                SlotId slotId = slotDesc.getId();
                if (slotId == null) {
                    continue;
                }
                for (int i = 0; i < exprList.size(); i++) {
                    if (exprList.get(i) instanceof SlotRef) {
                        SlotRef slotRef = (SlotRef) (exprList.get(i));
                        if (bitSet.get(i) && slotRef.getSlotId() == slotId) {
                            slotIdSet.add(slotId);
                            break;
                        }
                    } else if (exprList.get(i) instanceof FunctionCallExpr) {
                        List<SlotRef> slotRefs = getSlotRefChildren(exprList.get(i));
                        for (SlotRef slotRef : slotRefs) {
                            if (bitSet.get(i) && slotRef.getSlotId() == slotId) {
                                slotIdSet.add(slotId);
                                break;
                            }
                        }
                    }
                }
            }
            groupingIdList.add(slotIdSet);
        }

        this.repeatSlotIdList = buildIdSetList(groupingIdList);
        for (Set<Integer> s : this.repeatSlotIdList) {
            allSlotId.addAll(s);
        }
        this.groupingList = groupingInfo.genGroupingList(groupByClause.getGroupingExprs());
        tupleIds.add(outputTupleDesc.getId());
        for (TupleId id : tupleIds) {
            analyzer.getTupleDesc(id).setIsMaterialized(true);
        }
        computeTupleStatAndMemLayout(analyzer);
        computeStats(analyzer);
        createDefaultSmap(analyzer);
    }

    private List<SlotRef> getSlotRefChildren(Expr root) {
        List<SlotRef> result = new ArrayList<>();
        for (Expr child : root.getChildren()) {
            if (child instanceof SlotRef) {
                result.add((SlotRef) child);
            } else {
                result.addAll(getSlotRefChildren(child));
            }
        }
        return result;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.REPEAT_NODE;
        msg.repeat_node = new TRepeatNode(outputTupleDesc.getId().asInt(), repeatSlotIdList, groupingList.get(0),
                groupingList, allSlotId);
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("Repeat", repeatSlotIdList.size()).addValue(
                super.debugString()).toString();
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        if (detailLevel == TExplainLevel.BRIEF) {
            return "";
        }
        StringBuilder output = new StringBuilder();
        output.append(detailPrefix + "repeat: repeat ");
        output.append(repeatSlotIdList.size() - 1);
        output.append(" lines ");
        output.append(repeatSlotIdList);
        output.append("\n");
        if (CollectionUtils.isNotEmpty(outputTupleDesc.getSlots())) {
            output.append(detailPrefix + "generate: ");
            output.append(outputTupleDesc.getSlots().stream().map(slot -> "`" + slot.getColumn().getName() + "`")
                    .collect(Collectors.joining(", ")) + "\n");
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }
}
