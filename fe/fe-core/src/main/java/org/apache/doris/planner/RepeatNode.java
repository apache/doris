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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.GroupingInfo;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TRepeatNode;

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used for grouping sets.
 * It will add some new rows and a column of groupingId according to grouping sets info.
 */
public class RepeatNode extends PlanNode {

    private List<Set<Integer>> repeatSlotIdList;
    private Set<Integer> allSlotId;
    private List<List<Long>> groupingList;
    private GroupingInfo groupingInfo;

    /**
     * just for new Optimizer.
     */
    public RepeatNode(PlanNodeId id, PlanNode input, GroupingInfo groupingInfo,
            List<Set<Integer>> repeatSlotIdList, Set<Integer> allSlotId, List<List<Long>> groupingList) {
        super(id, groupingInfo.getOutputTupleDesc().getId().asList(), "REPEAT_NODE");
        this.children.add(input);
        this.groupingInfo = Objects.requireNonNull(groupingInfo, "groupingInfo can not be null");
        this.repeatSlotIdList = Objects.requireNonNull(repeatSlotIdList, "repeatSlotIdList can not be null");
        this.allSlotId = Objects.requireNonNull(allSlotId, "allSlotId can not be null");
        this.groupingList = Objects.requireNonNull(groupingList, "groupingList can not be null");
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.REPEAT_NODE;
        msg.repeat_node = new TRepeatNode(groupingInfo.getOutputTupleDesc().getId().asInt(), repeatSlotIdList,
                groupingList.get(0), groupingList, allSlotId, Expr.treesToThrift(groupingInfo.getPreRepeatExprs()));
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
        if (CollectionUtils.isNotEmpty(groupingInfo.getOutputTupleDesc().getSlots())) {
            output.append(detailPrefix + "output slots: ");
            output.append(groupingInfo.getOutputTupleDesc().getSlots().stream()
                    .map(slot -> "`" + slot.getLabel() + "`")
                    .collect(Collectors.joining(", ")) + "\n");
        }
        return output.toString();
    }

    // Determined by its child.
    @Override
    public boolean isSerialOperator() {
        return children.get(0).isSerialOperator();
    }

    public GroupingInfo getGroupingInfo() {
        return groupingInfo;
    }
}
