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
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.OrderByElement;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
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
        msg.analytic_node.setPartitionExprs(ExprToThriftVisitor.treesToThrift(partitionExprs));
        msg.analytic_node.setOrderByExprs(
                ExprToThriftVisitor.treesToThrift(OrderByElement.getOrderByExprs(orderByElements)));
        msg.analytic_node.setAnalyticFunctions(ExprToThriftVisitor.treesToThrift(analyticFnCalls));
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
            strings.add("[" + fnCall.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE) + "]");
        }

        output.append(Joiner.on(", ").join(strings));
        output.append("\n");

        if (!partitionExprs.isEmpty()) {
            output.append(prefix).append("partition by: ");
            strings.clear();

            for (Expr partitionExpr : partitionExprs) {
                strings.add(partitionExpr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
            }

            output.append(Joiner.on(", ").join(strings));
            output.append("\n");
        }

        if (!orderByElements.isEmpty()) {
            output.append(prefix).append("order by: ");
            strings.clear();

            for (OrderByElement element : orderByElements) {
                strings.add(orderByElementToSql(element));
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


    private String orderByElementToSql(OrderByElement orderByElement) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(orderByElement.getExpr().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
        strBuilder.append(orderByElement.getIsAsc() ? " ASC" : " DESC");

        // When ASC and NULLS LAST or DESC and NULLS FIRST, we do not print NULLS FIRST/LAST
        // because it is the default behavior and we want to avoid printing NULLS FIRST/LAST
        // whenever possible as it is incompatible with Hive (SQL compatibility with Hive is
        // important for views).
        if (orderByElement.getNullsFirstParam() != null) {
            if (orderByElement.getIsAsc() && orderByElement.getNullsFirstParam()) {
                // If ascending, nulls are last by default, so only add if nulls first.
                strBuilder.append(" NULLS FIRST");
            } else if (!orderByElement.getIsAsc() && !orderByElement.getNullsFirstParam()) {
                // If descending, nulls are first by default, so only add if nulls last.
                strBuilder.append(" NULLS LAST");
            }
        }

        return strBuilder.toString();
    }

    /**
     * If `partitionExprs` is empty, the result must be output by single instance.
     *
     * For example, for `window (colA order by colB)`,
     * all data should be input in this node to ensure the global ordering by colB.
     */
    @Override
    public boolean isSerialNode() {
        return partitionExprs.isEmpty();
    }

    /**
     * Mirrors BE's
     * {@code AnalyticSinkOperatorX::is_shuffled_operator() = !_partition_by_eq_expr_ctxs.empty()}
     * (be/src/exec/operator/analytic_sink_operator.h:226). With PARTITION BY, input must be
     * hash-partitioned by partition keys, so downstream UnionNode / SetOperationNode under
     * us must pre-shuffle their branches to match — the framework propagates this through
     * {@link PlanTranslatorContext#hasShuffleForCorrectnessAncestor}.
     */
    @Override
    public boolean requiresShuffleForCorrectness() {
        return !partitionExprs.isEmpty();
    }

    @Override
    protected List<Expr> getSemanticPartitionExprs() {
        return partitionExprs;
    }

    @Override
    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(PlanTranslatorContext translatorContext,
            PlanNode parent, LocalExchangeTypeRequire parentRequire) {
        LocalExchangeTypeRequire requireChild;
        LocalExchangeType outputType = null;
        if (partitionExprs.isEmpty()) {
            // Serial AnalyticEval (OVER() with no PARTITION BY):
            // Must NOT have any LocalExchange between AnalyticEval and its child.
            // On BE, AnalyticSink and AnalyticSource share state (source_deps/sink_deps).
            // A LocalExchange below would restore the AnalyticSink pipeline to _num_instances
            // tasks while the serial AnalyticSource pipeline stays at 1 task.
            //
            // Use enforceRequire with noRequire to traverse children, then strip any
            // LocalExchange the child inserted (e.g., Exchange wrapping itself with PASSTHROUGH).
            Pair<PlanNode, LocalExchangeType> enforceResult
                    = enforceRequire(translatorContext, children.get(0), 0, LocalExchangeTypeRequire.noRequire());
            PlanNode newChild = enforceResult.first;
            if (newChild instanceof LocalExchangeNode) {
                newChild = newChild.getChild(0);
            }
            children = Lists.newArrayList(newChild);
            // Return NOOP: the serial AnalyticSource pipeline has 1 task, we don't provide
            // fan-out ourselves. The parent's enforceRequire framework-level serial check
            // will see our serial status and insert PASSTHROUGH LE above us if needed.
            return Pair.of(this, LocalExchangeType.NOOP);
        } else if (orderByElements.isEmpty()) {
            if (AddLocalExchange.isColocated(this)) {
                requireChild = LocalExchangeTypeRequire.requireHash();
                outputType = AddLocalExchange.resolveExchangeType(
                        LocalExchangeTypeRequire.requireHash());
            } else {
                // Non-colocated analytic with PARTITION BY but no ORDER BY:
                // The parent SortNode (mergeByExchange) will insert PASSTHROUGH above us,
                // which is what BE does natively. Don't force a hash exchange here.
                requireChild = LocalExchangeTypeRequire.noRequire();
                outputType = LocalExchangeType.NOOP;
            }
        } else if (children.get(0).isSerialOperatorOnBe(translatorContext.getConnectContext())) {
            // BE base class: _child->is_serial_operator() ? PASSTHROUGH : NOOP
            requireChild = LocalExchangeTypeRequire.requirePassthrough();
            outputType = LocalExchangeType.PASSTHROUGH;
        } else {
            requireChild = LocalExchangeTypeRequire.noRequire();
            outputType = LocalExchangeType.NOOP;
        }

        Pair<PlanNode, LocalExchangeType> enforceResult
                = enforceRequire(translatorContext, children.get(0), 0, requireChild);
        children = Lists.newArrayList(enforceResult.first);
        if (outputType == null) {
            outputType = enforceResult.second;
        }
        return Pair.of(this, outputType);
    }

    @Override
    protected boolean shouldResetSerialFlagForChild(int childIndex) {
        return true;
    }
}
