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
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.OrderByElement;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.thrift.TAnalyticNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Computation of analytic exprs.
 */
public class AnalyticEvalNode extends PlanNode {
    private static final Logger LOG = LoggerFactory.getLogger(AnalyticEvalNode.class);

    private List<Expr> analyticFnCalls;

    // Partitioning exprs from the AnalyticInfo
    private final List<Expr> partitionExprs;

    // TODO: Remove when the BE uses partitionByLessThan rather than the exprs
    private List<Expr> substitutedPartitionExprs;
    private List<OrderByElement> orderByElements;

    private final AnalyticWindow analyticWindow;

    // Physical tuples used/produced by this analytic node.
    private final TupleDescriptor intermediateTupleDesc;
    private final TupleDescriptor outputTupleDesc;

    // maps from the logical output slots in logicalTupleDesc_ to their corresponding
    // physical output slots in outputTupleDesc_
    private final ExprSubstitutionMap logicalToPhysicalSmap;

    // predicates constructed from partitionExprs_/orderingExprs_ to
    // compare input to buffered tuples
    private final Expr partitionByEq;
    private final Expr orderByEq;
    private final TupleDescriptor bufferedTupleDesc;

    private boolean isColocate = false;

    public AnalyticEvalNode(
            PlanNodeId id, PlanNode input, List<Expr> analyticFnCalls,
            List<Expr> partitionExprs, List<OrderByElement> orderByElements,
            AnalyticWindow analyticWindow, TupleDescriptor intermediateTupleDesc,
            TupleDescriptor outputTupleDesc, ExprSubstitutionMap logicalToPhysicalSmap,
            Expr partitionByEq, Expr orderByEq, TupleDescriptor bufferedTupleDesc) {
        super(id, Lists.newArrayList(input.getOutputTupleIds()), "ANALYTIC", StatisticalType.ANALYTIC_EVAL_NODE);
        Preconditions.checkState(!tupleIds.contains(outputTupleDesc.getId()));
        // we're materializing the input row augmented with the analytic output tuple
        tupleIds.add(outputTupleDesc.getId());
        this.analyticFnCalls = analyticFnCalls;
        this.partitionExprs = partitionExprs;
        this.orderByElements = orderByElements;
        this.analyticWindow = analyticWindow;
        this.intermediateTupleDesc = intermediateTupleDesc;
        this.outputTupleDesc = outputTupleDesc;
        this.logicalToPhysicalSmap = logicalToPhysicalSmap;
        this.partitionByEq = partitionByEq;
        this.orderByEq = orderByEq;
        this.bufferedTupleDesc = bufferedTupleDesc;
        children.add(input);
        nullableTupleIds = Sets.newHashSet(input.getNullableTupleIds());
    }

    // constructor used in Nereids
    public AnalyticEvalNode(
            PlanNodeId id, PlanNode input, List<Expr> analyticFnCalls,
            List<Expr> partitionExprs, List<OrderByElement> orderByElements,
            AnalyticWindow analyticWindow, TupleDescriptor intermediateTupleDesc,
            TupleDescriptor outputTupleDesc, Expr partitionByEq, Expr orderByEq,
            TupleDescriptor bufferedTupleDesc) {
        super(id,
                (input.getOutputTupleDesc() != null
                        ? Lists.newArrayList(input.getOutputTupleDesc().getId()) :
                        input.getTupleIds()),
                "ANALYTIC", StatisticalType.ANALYTIC_EVAL_NODE);
        Preconditions.checkState(!tupleIds.contains(outputTupleDesc.getId()));
        // we're materializing the input row augmented with the analytic output tuple
        tupleIds.add(outputTupleDesc.getId());
        this.analyticFnCalls = analyticFnCalls;
        this.partitionExprs = partitionExprs;
        this.substitutedPartitionExprs = partitionExprs;
        this.orderByElements = orderByElements;
        this.analyticWindow = analyticWindow;
        this.intermediateTupleDesc = intermediateTupleDesc;
        this.outputTupleDesc = outputTupleDesc;
        this.logicalToPhysicalSmap = new ExprSubstitutionMap();
        this.partitionByEq = partitionByEq;
        this.orderByEq = orderByEq;
        this.bufferedTupleDesc = bufferedTupleDesc;
        children.add(input);
        nullableTupleIds = Sets.newHashSet(input.getNullableTupleIds());
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        analyzer.getDescTbl().computeStatAndMemLayout();
        intermediateTupleDesc.computeStatAndMemLayout();
        // we add the analyticInfo's smap to the combined smap of our child
        outputSmap = logicalToPhysicalSmap;
        createDefaultSmap(analyzer);

        // Do not assign any conjuncts here: the conjuncts out of our SelectStmt's
        // Where clause have already been assigned, and conjuncts coming out of an
        // enclosing scope need to be evaluated *after* all analytic computations.

        // do this at the end so it can take all conjuncts into account
        computeStats(analyzer);
        if (LOG.isDebugEnabled()) {
            LOG.debug("desctbl: " + analyzer.getDescTbl().debugString());
        }

        // point fn calls, partition and ordering exprs at our input
        ExprSubstitutionMap childSmap = getCombinedChildSmap();
        analyticFnCalls = Expr.substituteList(analyticFnCalls, childSmap, analyzer, false);
        substitutedPartitionExprs = Expr.substituteList(partitionExprs, childSmap,
                                    analyzer, false);
        orderByElements = OrderByElement.substitute(orderByElements, childSmap, analyzer);
        if (LOG.isDebugEnabled()) {
            LOG.debug("evalnode: " + debugString());
        }
    }

    @Override
    protected void computeStats(Analyzer analyzer) throws UserException {
        super.computeStats(analyzer);
        if (!analyzer.safeIsEnableJoinReorderBasedCost()) {
            return;
        }
        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = (long) statsDeriveResult.getRowCount();
    }

    @Override
    protected void computeOldCardinality() {
        cardinality = getChild(0).cardinality;
    }

    public void setColocate(boolean colocate) {
        this.isColocate = colocate;
    }

    @Override
    protected String debugString() {
        List<String> orderByElementStrs = Lists.newArrayList();

        for (OrderByElement element : orderByElements) {
            orderByElementStrs.add(element.toSql());
        }

        return MoreObjects.toStringHelper(this)
               .add("analyticFnCalls", Expr.debugString(analyticFnCalls))
               .add("partitionExprs", Expr.debugString(partitionExprs))
               .add("substitutedPartitionExprs", Expr.debugString(substitutedPartitionExprs))
               .add("orderByElements", Joiner.on(", ").join(orderByElementStrs))
               .add("window", analyticWindow)
               .add("intermediateTid", intermediateTupleDesc.getId())
               .add("intermediateTid", outputTupleDesc.getId())
               .add("outputTid", outputTupleDesc.getId())
               .add("partitionByEq",
                    partitionByEq != null ? partitionByEq.debugString() : "null")
               .add("orderByEq",
                    orderByEq != null ? orderByEq.debugString() : "null")
               .addValue(super.debugString())
               .toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ANALYTIC_EVAL_NODE;
        msg.analytic_node = new TAnalyticNode();
        msg.analytic_node.setIntermediateTupleId(intermediateTupleDesc.getId().asInt());
        msg.analytic_node.setOutputTupleId(outputTupleDesc.getId().asInt());
        msg.analytic_node.setPartitionExprs(Expr.treesToThrift(substitutedPartitionExprs));
        msg.analytic_node.setOrderByExprs(Expr.treesToThrift(OrderByElement.getOrderByExprs(orderByElements)));
        msg.analytic_node.setAnalyticFunctions(Expr.treesToThrift(analyticFnCalls));
        msg.analytic_node.setIsColocate(isColocate);
        if (analyticWindow == null) {
            if (!orderByElements.isEmpty()) {
                msg.analytic_node.setWindow(AnalyticWindow.DEFAULT_WINDOW.toThrift());
            }
        } else {
            // TODO: Window boundaries should have range_offset_predicate set
            msg.analytic_node.setWindow(analyticWindow.toThrift());
        }

        if (partitionByEq != null) {
            msg.analytic_node.setPartitionByEq(partitionByEq.treeToThrift());
        }

        if (orderByEq != null) {
            msg.analytic_node.setOrderByEq(orderByEq.treeToThrift());
        }

        if (bufferedTupleDesc != null) {
            msg.analytic_node.setBufferedTupleId(bufferedTupleDesc.getId().asInt());
        }
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
}
