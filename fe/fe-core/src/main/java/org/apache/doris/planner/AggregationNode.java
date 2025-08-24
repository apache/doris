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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AggregationNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TAggregationNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TNormalizedAggregateNode;
import org.apache.doris.thrift.TNormalizedPlanNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TSortInfo;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Aggregation computation.
 */
public class AggregationNode extends PlanNode {
    private final AggregateInfo aggInfo;

    // Set to true if this aggregation node needs to run the Finalize step. This
    // node is the root node of a distributed aggregation.
    private boolean needsFinalize;
    private boolean isColocate = false;

    // If true, use streaming preaggregation algorithm. Not valid if this is a merge agg.
    private boolean useStreamingPreagg;

    private SortInfo sortByGroupKey;

    /**
     * Create an agg node that is not an intermediate node.
     * isIntermediate is true if it is a slave node in a 2-part agg plan.
     */
    public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
        super(id, aggInfo.getOutputTupleId().asList(), "AGGREGATE", StatisticalType.AGG_NODE);
        this.aggInfo = aggInfo;
        this.children.add(input);
        this.needsFinalize = true;
        updateplanNodeName();
    }

    public AggregateInfo getAggInfo() {
        return aggInfo;
    }

    // Unsets this node as requiring finalize. Only valid to call this if it is
    // currently marked as needing finalize.
    public void unsetNeedsFinalize() {
        Preconditions.checkState(needsFinalize);
        needsFinalize = false;
        updateplanNodeName();
    }

    // Used by new optimizer
    public void setUseStreamingPreagg(boolean useStreamingPreagg) {
        this.useStreamingPreagg = useStreamingPreagg;
    }

    @Override
    public void setCompactData(boolean on) {
        this.compactData = on;
    }

    private void updateplanNodeName() {
        StringBuilder sb = new StringBuilder();
        sb.append("VAGGREGATE");
        sb.append(" (");
        if (aggInfo.isMerge()) {
            sb.append("merge");
        } else {
            sb.append("update");
        }
        if (needsFinalize) {
            sb.append(" finalize");
        } else {
            sb.append(" serialize");
        }
        sb.append(")");
        setPlanNodeName(sb.toString());
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("aggInfo", aggInfo.debugString()).addValue(
          super.debugString()).toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        aggInfo.updateMaterializedSlots();
        msg.node_type = TPlanNodeType.AGGREGATION_NODE;
        List<TExpr> aggregateFunctions = Lists.newArrayList();
        List<TSortInfo> aggSortInfos = Lists.newArrayList();
        // only serialize agg exprs that are being materialized
        for (FunctionCallExpr e : aggInfo.getMaterializedAggregateExprs()) {
            aggregateFunctions.add(e.treeToThrift());
            List<TExpr> orderingExpr = Lists.newArrayList();
            List<Boolean> isAscs = Lists.newArrayList();
            List<Boolean> nullFirsts = Lists.newArrayList();

            e.getOrderByElements().forEach(o -> {
                orderingExpr.add(o.getExpr().treeToThrift());
                isAscs.add(o.getIsAsc());
                nullFirsts.add(o.getNullsFirstParam());
            });
            aggSortInfos.add(new TSortInfo(orderingExpr, isAscs, nullFirsts));
        }

        msg.agg_node = new TAggregationNode(
                aggregateFunctions,
                aggInfo.getOutputTupleId().asInt(),
                aggInfo.getOutputTupleId().asInt(), needsFinalize);
        msg.agg_node.setAggSortInfos(aggSortInfos);
        msg.agg_node.setUseStreamingPreaggregation(useStreamingPreagg);
        msg.agg_node.setIsFirstPhase(aggInfo.isFirstPhase());
        msg.agg_node.setIsColocate(isColocate);
        if (sortByGroupKey != null) {
            msg.agg_node.setAggSortInfoByGroupKey(sortByGroupKey.toThrift());
        }
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        if (groupingExprs != null) {
            msg.agg_node.setGroupingExprs(Expr.treesToThrift(groupingExprs));
        }
        
        // Check if this aggregation has empty grouping from GROUPING SETS/ROLLUP/CUBE
        boolean hasEmptyGrouping = hasEmptyGroupingFromRepeatNode();
        msg.agg_node.setHasEmptyGrouping(hasEmptyGrouping);
    }

    @Override
    public void normalize(TNormalizedPlanNode normalizedPlan, Normalizer normalizer) {
        TNormalizedAggregateNode normalizedAggregateNode = new TNormalizedAggregateNode();

        // if (aggInfo.getGroupingExprs().size() > 3) {
        //     throw new IllegalStateException("Too many grouping expressions, not use query cache");
        // }

        normalizedAggregateNode.setOutputTupleId(
                normalizer.normalizeTupleId(aggInfo.getOutputTupleId().asInt()));
        normalizedAggregateNode.setGroupingExprs(normalizeExprs(aggInfo.getGroupingExprs(), normalizer));
        normalizedAggregateNode.setAggregateFunctions(normalizeExprs(aggInfo.getAggregateExprs(), normalizer));
        normalizedAggregateNode.setIsFinalize(needsFinalize);
        normalizedAggregateNode.setUseStreamingPreaggregation(useStreamingPreagg);

        normalizeAggOutputProjects(normalizedAggregateNode, normalizer);

        normalizedPlan.setNodeType(TPlanNodeType.AGGREGATION_NODE);
        normalizedPlan.setAggregationNode(normalizedAggregateNode);
    }

    @Override
    protected void normalizeProjects(TNormalizedPlanNode normalizedPlanNode, Normalizer normalizer) {
        List<SlotDescriptor> outputSlots =
                getOutputTupleIds()
                        .stream()
                        .flatMap(tupleId -> normalizer.getDescriptorTable().getTupleDesc(tupleId).getSlots().stream())
                        .collect(Collectors.toList());

        List<Expr> projectList = this.projectList;
        if (projectList == null) {
            projectList = this.aggInfo.getOutputTupleDesc()
                    .getSlots()
                    .stream()
                    .map(SlotRef::new)
                    .collect(Collectors.toList());
        }

        List<TExpr> projectThrift = normalizeProjects(outputSlots, projectList, normalizer);
        normalizedPlanNode.setProjects(projectThrift);
    }

    private void normalizeAggOutputProjects(TNormalizedAggregateNode aggregateNode, Normalizer normalizer) {
        List<Expr> projectToIntermediateTuple = ImmutableList.<Expr>builder()
                .addAll(aggInfo.getGroupingExprs())
                .addAll(aggInfo.getAggregateExprs())
                .build();

        List<SlotDescriptor> intermediateSlots = aggInfo.getOutputTupleDesc().getSlots();
        List<TExpr> projects = normalizeProjects(intermediateSlots, projectToIntermediateTuple, normalizer);
        aggregateNode.setProjectToAggOutputTuple(projects);
    }

    /**
     * Check if this aggregation node has empty grouping from GROUPING SETS/ROLLUP/CUBE.
     * This is determined by checking if the immediate child is a RepeatNode and
     * if that RepeatNode contains empty grouping sets (represented by grouping_list with all 1s).
     */
    private boolean hasEmptyGroupingFromRepeatNode() {
        // Check if the immediate child is a RepeatNode
        if (children.size() == 1 && children.get(0) instanceof RepeatNode) {
            RepeatNode repeatNode = (RepeatNode) children.get(0);
            // Check if RepeatNode contains empty grouping 
            // Empty grouping is represented by having at least one grouping set where all values are 1
            // (indicating all grouping columns are masked/null)
            return repeatNode.hasEmptyGrouping();
        }
        return false;
    }

    protected String getDisplayLabelDetail() {
        if (useStreamingPreagg) {
            return "STREAMING";
        }
        return null;
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        aggInfo.updateMaterializedSlots();
        StringBuilder output = new StringBuilder();
        String nameDetail = getDisplayLabelDetail();
        if (nameDetail != null) {
            output.append(detailPrefix).append(nameDetail).append("\n");
        }

        if (detailLevel == TExplainLevel.BRIEF) {
            output.append(detailPrefix).append(String.format(
                    "cardinality=%,d",  cardinality)).append("\n");
            return output.toString();
        }

        if (aggInfo.getAggregateExprs() != null && aggInfo.getMaterializedAggregateExprs().size() > 0) {
            List<String> labels = aggInfo.getMaterializedAggregateExprLabels();
            if (labels.isEmpty()) {
                output.append(detailPrefix).append("output: ")
                        .append(getExplainString(aggInfo.getMaterializedAggregateExprs())).append("\n");
            } else {
                output.append(detailPrefix).append("output: ")
                        .append(StringUtils.join(labels, ", ")).append("\n");
            }
        }
        // TODO: group by can be very long. Break it into multiple lines
        output.append(detailPrefix).append("group by: ")
                .append(getExplainString(aggInfo.getGroupingExprs()))
                .append("\n");
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("having: ").append(getExplainString(conjuncts)).append("\n");
        }
        output.append(detailPrefix).append("sortByGroupKey:").append(sortByGroupKey != null).append("\n");
        output.append(detailPrefix).append(String.format(
                "cardinality=%,d", cardinality)).append("\n");
        return output.toString();
    }

    // If `GroupingExprs` is empty and agg need to finalize, the result must be output by single instance
    @Override
    public boolean isSerialOperator() {
        return aggInfo.getGroupingExprs().isEmpty() && needsFinalize;
    }

    public void setColocate(boolean colocate) {
        isColocate = colocate;
    }

    public void setSortByGroupKey(SortInfo sortByGroupKey) {
        this.sortByGroupKey = sortByGroupKey;
    }
}
