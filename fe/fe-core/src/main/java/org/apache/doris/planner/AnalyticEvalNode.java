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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AnalyticEvalNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.AnalyticWindow;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.OrderByElement;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.thrift.TAnalyticNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Computation of analytic exprs.
 */
public class AnalyticEvalNode extends PlanNode {

    private List<Expr> analyticFnCalls;

    // Partitioning exprs from the AnalyticInfo
    private final List<Expr> partitionExprs;

    private List<OrderByElement> orderByElements;

    private final AnalyticWindow analyticWindow;

    // Physical tuples used/produced by this analytic node.
    private final TupleDescriptor outputTupleDesc;

    private boolean isColocate = false;

    // constructor used in Nereids
    public AnalyticEvalNode(
            PlanNodeId id, PlanNode input, List<Expr> analyticFnCalls,
            List<Expr> partitionExprs, List<OrderByElement> orderByElements,
            AnalyticWindow analyticWindow, TupleDescriptor outputTupleDesc) {
        super(id, input.getOutputTupleIds(), "ANALYTIC");
        Preconditions.checkState(!tupleIds.contains(outputTupleDesc.getId()));
        // we're materializing the input row augmented with the analytic output tuple
        tupleIds.add(outputTupleDesc.getId());
        this.analyticFnCalls = analyticFnCalls;
        this.partitionExprs = partitionExprs;
        this.orderByElements = orderByElements;
        this.analyticWindow = analyticWindow;
        this.outputTupleDesc = outputTupleDesc;
        children.add(input);
    }

    public void setColocate(boolean colocate) {
        this.isColocate = colocate;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ANALYTIC_EVAL_NODE;
        msg.analytic_node = new TAnalyticNode();
        msg.analytic_node.setIntermediateTupleId(outputTupleDesc.getId().asInt());
        msg.analytic_node.setOutputTupleId(outputTupleDesc.getId().asInt());
        msg.analytic_node.setPartitionExprs(Expr.treesToThrift(partitionExprs));
        msg.analytic_node.setOrderByExprs(Expr.treesToThrift(OrderByElement.getOrderByExprs(orderByElements)));
        msg.analytic_node.setAnalyticFunctions(Expr.treesToThrift(analyticFnCalls));
        msg.analytic_node.setIsColocate(isColocate);
        // TODO: Window boundaries should have range_offset_predicate set
        msg.analytic_node.setWindow(analyticWindow.toThrift());
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        if (detailLevel == TExplainLevel.BRIEF) {
            return "";
        }
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("functions: ");
        List<String> strings = Lists.newArrayList();

        for (Expr fnCall : analyticFnCalls) {
            strings.add("[" + fnCall.toSql() + "]");
        }

        output.append(Joiner.on(", ").join(strings));
        output.append("\n");

        if (!partitionExprs.isEmpty()) {
            output.append(prefix).append("partition by: ");
            strings.clear();

            for (Expr partitionExpr : partitionExprs) {
                strings.add(partitionExpr.toSql());
            }

            output.append(Joiner.on(", ").join(strings));
            output.append("\n");
        }

        if (!orderByElements.isEmpty()) {
            output.append(prefix).append("order by: ");
            strings.clear();

            for (OrderByElement element : orderByElements) {
                strings.add(element.toSql());
            }

            output.append(Joiner.on(", ").join(strings));
            output.append("\n");
        }

        if (analyticWindow != null) {
            output.append(prefix + "window: ");
            output.append(analyticWindow.toSql());
            output.append("\n");
        }

        if (!conjuncts.isEmpty()) {
            output.append(prefix + "predicates: " + getExplainString(conjuncts) + "\n");
        }

        return output.toString();
    }

    /**
     * If `partitionExprs` is empty, the result must be output by single instance.
     *
     * For example, for `window (colA order by colB)`,
     * all data should be input in this node to ensure the global ordering by colB.
     */
    @Override
    public boolean isSerialOperator() {
        return partitionExprs.isEmpty();
    }
}
