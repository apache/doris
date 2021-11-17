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
import org.apache.doris.analysis.LateralViewRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TTableFunctionNode;

import java.util.List;
import java.util.stream.Collectors;

public class TableFunctionNode extends PlanNode {

    private List<LateralViewRef> lateralViewRefs;
    private List<FunctionCallExpr> fnCallExprList;
    private List<TupleId> lateralViewTupleIds;

    protected TableFunctionNode(PlanNodeId id, PlanNode inputNode, List<LateralViewRef> lateralViewRefs) {
        super(id, "TABLE FUNCTION NODE");
        tupleIds.addAll(inputNode.getTupleIds());
        tblRefIds.addAll(inputNode.getTupleIds());
        lateralViewTupleIds = lateralViewRefs.stream().map(e -> e.getDesc().getId())
                .collect(Collectors.toList());
        tupleIds.addAll(lateralViewTupleIds);
        tblRefIds.addAll(lateralViewTupleIds);
        children.add(inputNode);
        this.lateralViewRefs = lateralViewRefs;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        fnCallExprList = lateralViewRefs.stream().map(e -> e.getFnExpr()).collect(Collectors.toList());
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
        output.append(prefix + "table function: ");
        for (FunctionCallExpr fnExpr : fnCallExprList) {
            output.append(fnExpr.toSqlImpl() + " ");
        }
        output.append("\n");

        output.append(prefix + "lateral view tuple id: ");
        for (TupleId tupleId : lateralViewTupleIds) {
            output.append(tupleId.asInt() + " ");
        }
        output.append("\n");

        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }

        output.append(prefix + "tuple id: ");
        for (TupleId tupleId : tupleIds) {
            output.append(tupleId.asInt() + " ");
        }
        output.append("\n");

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
        msg.table_function_node.setFnCallExprList(Expr.treesToThrift(fnCallExprList));
    }
}
