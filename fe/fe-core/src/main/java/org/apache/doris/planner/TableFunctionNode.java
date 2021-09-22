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
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.LateralViewRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TTableFunctionNode;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;

public class TableFunctionNode extends PlanNode {

    private List<LateralViewRef> lateralViewRefList;

    private List<FunctionCallExpr> fnCallExprList;
    private List<TupleId> inputTupleIds;

    protected TableFunctionNode(PlanNodeId id, PlanNode inputScanNode, List<LateralViewRef> lateralViewRefList) {
        super(id, "TABLE FUNCTION NODE");
        this.inputTupleIds = inputScanNode.getTupleIds();
        tupleIds.addAll(inputScanNode.getTupleIds());
        tblRefIds.addAll(inputScanNode.getTupleIds());
        for (LateralViewRef lateralViewRef : lateralViewRefList) {
            tupleIds.add(lateralViewRef.getDesc().getId());
            tblRefIds.add(lateralViewRef.getDesc().getId());
        }
        children.add(inputScanNode);
        this.lateralViewRefList = lateralViewRefList;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        fnCallExprList = Lists.newArrayList();
        for (LateralViewRef lateralViewRef: lateralViewRefList) {
            fnCallExprList.add(lateralViewRef.getFnExpr());
        }
        computeStats(analyzer);
    }

    @Override
    protected void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        // TODO the cardinality = child cardinality * cardinality of list column
        cardinality = children.get(0).cardinality;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix + "table function: ").append(getExplainString(fnCallExprList) + "\n");
        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }

        output.append(prefix + "input tuple ids: ").append(Joiner.on(",").join(inputTupleIds) + "\n");
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
        }
        output.append(prefix).append(String.format("cardinality=%s", cardinality)).append("\n");
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.TABLE_FUNCTION_NODE;
        msg.table_function_node = new TTableFunctionNode();
        for (FunctionCallExpr functionCallExpr : fnCallExprList) {
            msg.table_function_node.addToFnCallExprList(functionCallExpr.treeToThrift());
        }
        for (TupleId tupleId : inputTupleIds) {
            msg.table_function_node.addToInputTupleIds(tupleId.asInt());
        }
    }
}
