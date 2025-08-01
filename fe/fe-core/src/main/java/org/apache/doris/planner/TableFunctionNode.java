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
import org.apache.doris.analysis.LateralViewRef;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TTableFunctionNode;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class TableFunctionNode extends PlanNode {
    private List<LateralViewRef> lateralViewRefs;
    private ArrayList<Expr> fnCallExprList;
    private List<TupleId> lateralViewTupleIds;

    // The output slot ids of TableFunctionNode
    // Only the slot whose id is in this list will be output by TableFunctionNode
    private List<SlotId> outputSlotIds = Lists.newArrayList();

    public TableFunctionNode(PlanNodeId id, PlanNode inputNode, TupleId lateralViewTupleId,
            ArrayList<Expr> fnCallExprList, List<SlotId> outputSlotIds) {
        super(id, "TABLE FUNCTION NODE", StatisticalType.TABLE_FUNCTION_NODE);
        if (inputNode.outputTupleDesc != null) {
            tupleIds.add(inputNode.outputTupleDesc.getId());
        } else {
            List<TupleId> childOutputTupleIds = inputNode.getOutputTupleIds();
            if (childOutputTupleIds != null && !childOutputTupleIds.isEmpty()) {
                tupleIds.addAll(childOutputTupleIds);
            } else {
                tupleIds.addAll(inputNode.getTupleIds());
            }
        }
        tupleIds.add(lateralViewTupleId);
        this.lateralViewTupleIds = Lists.newArrayList(lateralViewTupleId);
        this.fnCallExprList = fnCallExprList;
        this.outputSlotIds = outputSlotIds;
        this.children.add(inputNode);
    }

    public void setOutputSlotIds(List<SlotId> outputSlotIds) {
        this.outputSlotIds = outputSlotIds;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("table function: ");
        for (Expr fnExpr : fnCallExprList) {
            output.append(fnExpr.toSql()).append(" ");
        }
        output.append("\n");

        output.append(prefix).append("lateral view tuple id: ");
        for (TupleId tupleId : lateralViewTupleIds) {
            output.append(tupleId.asInt()).append(" ");
        }
        output.append("\n");

        if (detailLevel == TExplainLevel.BRIEF) {
            output.append(prefix).append(String.format("cardinality=%,d", cardinality)).append("\n");
            return output.toString();
        }

        output.append(prefix).append("output slot id: ");
        for (SlotId slotId : outputSlotIds) {
            output.append(slotId.asInt()).append(" ");
        }
        output.append("\n");

        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
        }
        output.append(prefix).append(String.format("cardinality=%,d", cardinality)).append("\n");
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.TABLE_FUNCTION_NODE;
        msg.table_function_node = new TTableFunctionNode();
        msg.table_function_node.setFnCallExprList(Expr.treesToThrift(fnCallExprList));
        for (SlotId slotId : outputSlotIds) {
            msg.table_function_node.addToOutputSlotIds(slotId.asInt());
        }
    }
}
