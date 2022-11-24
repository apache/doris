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
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.GroupByClause;
import org.apache.doris.analysis.GroupingInfo;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
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
import java.util.Objects;
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
        super(id, groupingInfo.getOutputTupleDesc().getId().asList(), "REPEAT_NODE", StatisticalType.REPEAT_NODE);
        this.children.add(input);
        this.groupingInfo = groupingInfo;
        this.input = input;
        this.groupByClause = groupByClause;
    }

    /**
     * just for new Optimizer.
     */
    public RepeatNode(PlanNodeId id, PlanNode input, GroupingInfo groupingInfo,
            List<Set<Integer>> repeatSlotIdList, Set<Integer> allSlotId, List<List<Long>> groupingList) {
        super(id, groupingInfo.getOutputTupleDesc().getId().asList(), "REPEAT_NODE", StatisticalType.REPEAT_NODE);
        this.children.add(input);
        this.groupingInfo = Objects.requireNonNull(groupingInfo, "groupingInfo can not be null");
        this.input = Objects.requireNonNull(input, "input can not bu null");
        this.repeatSlotIdList = Objects.requireNonNull(repeatSlotIdList, "repeatSlotIdList can not be null");
        this.allSlotId = Objects.requireNonNull(allSlotId, "allSlotId can not be null");
        this.groupingList = Objects.requireNonNull(groupingList, "groupingList can not be null");
        this.outputTupleDesc = groupingInfo.getOutputTupleDesc();
    }

    // only for unittest
    protected RepeatNode(PlanNodeId id, PlanNode input, List<Set<SlotId>> repeatSlotIdList,
            TupleDescriptor outputTupleDesc, List<List<Long>> groupingList) {
        super(id, input.getTupleIds(), "REPEAT_NODE", StatisticalType.REPEAT_NODE);
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
    public void computeStats(Analyzer analyzer) throws UserException {
        avgRowSize = 0;
        numNodes = 1;

        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = (long) statsDeriveResult.getRowCount();

        if (LOG.isDebugEnabled()) {
            LOG.debug("stats Sort: cardinality=" + cardinality);
        }
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        Preconditions.checkState(conjuncts.isEmpty());
        ExprSubstitutionMap childSmap = getCombinedChildSmap();
        groupByClause.substituteGroupingExprs(groupingInfo.getVirtualSlotRefs(), childSmap, analyzer);
        groupingInfo.substitutePreRepeatExprs(childSmap, analyzer);
        outputSmap = groupingInfo.getOutputTupleSmap();
        conjuncts = Expr.substituteList(conjuncts, outputSmap, analyzer, false);
        outputTupleDesc = groupingInfo.getOutputTupleDesc();
        List<TupleId> inputTupleIds = input.getOutputTupleIds();
        if (inputTupleIds.size() == 1) {
            // used for MaterializedViewSelector getTableIdToColumnNames
            outputTupleDesc.setTable(analyzer.getTupleDesc(inputTupleIds.get(0)).getTable());
        }

        outputTupleDesc.computeStatAndMemLayout();

        List<Set<SlotId>> groupingIdList = new ArrayList<>();
        List<SlotDescriptor> groupingSlotDescList = groupingInfo.getGroupingSlotDescList();
        for (BitSet bitSet : Collections.unmodifiableList(groupingInfo.getGroupingIdList())) {
            Set<SlotId> slotIdSet = new HashSet<>();
            for (int i = 0; i < groupingSlotDescList.size(); i++) {
                if (bitSet.get(i)) {
                    slotIdSet.add(groupingSlotDescList.get(i).getId());
                }
            }
            groupingIdList.add(slotIdSet);
        }

        this.repeatSlotIdList = buildIdSetList(groupingIdList);
        allSlotId = new HashSet<>();
        for (Set<Integer> s : this.repeatSlotIdList) {
            allSlotId.addAll(s);
        }
        this.groupingList = groupingInfo.genGroupingList(groupByClause.getGroupingExprs());
        for (TupleId id : tupleIds) {
            analyzer.getTupleDesc(id).setIsMaterialized(true);
        }
        computeTupleStatAndMemLayout(analyzer);
        computeStats(analyzer);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.REPEAT_NODE;
        msg.repeat_node =
                new TRepeatNode(outputTupleDesc.getId().asInt(), repeatSlotIdList, groupingList.get(0), groupingList,
                        allSlotId, Expr.treesToThrift(groupingInfo.getPreRepeatExprs()));
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("Repeat", repeatSlotIdList.size()).addValue(super.debugString())
                .toString();
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
        output.append(repeatSlotIdList).append("\n");
        output.append(detailPrefix).append("exprs: ").append(getExplainString(groupingInfo.getPreRepeatExprs()));
        output.append("\n");
        if (CollectionUtils.isNotEmpty(outputTupleDesc.getSlots())) {
            output.append(detailPrefix + "output slots: ");
            output.append(outputTupleDesc.getSlots().stream().map(slot -> "`" + slot.getLabel() + "`")
                    .collect(Collectors.joining(", ")) + "\n");
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }
}
