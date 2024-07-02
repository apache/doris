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
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.thrift.TAggregationNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TSortInfo;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Aggregation computation.
 */
public class AggregationNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(AggregationNode.class);
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

    /**
     * Copy c'tor used in clone().
     */
    private AggregationNode(PlanNodeId id, AggregationNode src) {
        super(id, src, "AGGREGATE", StatisticalType.AGG_NODE);
        aggInfo = src.aggInfo;
        needsFinalize = src.needsFinalize;
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

    /**
     * Sets this node as a preaggregation. Only valid to call this if it is not marked
     * as a preaggregation
     */
    public void setIsPreagg(PlannerContext ctx) {
        useStreamingPreagg =  ctx.getQueryOptions().isSetDisableStreamPreaggregations()
                && !ctx.getQueryOptions().disable_stream_preaggregations
                && aggInfo.getGroupingExprs().size() > 0;
    }

    // Used by new optimizer
    public void setNeedsFinalize(boolean needsFinalize) {
        this.needsFinalize = needsFinalize;
    }

    // Used by new optimizer
    public void setUseStreamingPreagg(boolean useStreamingPreagg) {
        this.useStreamingPreagg = useStreamingPreagg;
    }

    @Override
    public void setCompactData(boolean on) {
        this.compactData = on;
    }

    /**
     * Have this node materialize the aggregation's intermediate tuple instead of
     * the output tuple.
     */
    public void setIntermediateTuple() {
        Preconditions.checkState(!tupleIds.isEmpty());
        Preconditions.checkState(tupleIds.get(0).equals(aggInfo.getOutputTupleId()));
        tupleIds.clear();
        tupleIds.add(aggInfo.getIntermediateTupleId());
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        // Assign predicates to the top-most agg in the single-node plan that can evaluate
        // them, as follows: For non-distinct aggs place them in the 1st phase agg node. For
        // distinct aggs place them in the 2nd phase agg node. The conjuncts are
        // transferred to the proper place in the multi-node plan via transferConjuncts().
        if (tupleIds.get(0).equals(aggInfo.getResultTupleId()) && !aggInfo.isMerge()) {
            // Ignore predicates bound by a grouping slot produced by a SlotRef grouping expr.
            // Those predicates are already evaluated below this agg node (e.g., in a scan),
            // because the grouping slot must be in the same equivalence class as another slot
            // below this agg node. We must not ignore other grouping slots in order to retain
            // conjuncts bound by those grouping slots in createEquivConjuncts() (IMPALA-2089).
            // Those conjuncts cannot be redundant because our equivalence classes do not
            // capture dependencies with non-SlotRef exprs.
            // Set<SlotId> groupBySlots = Sets.newHashSet();
            // for (int i = 0; i < aggInfo.getGroupingExprs().size(); ++i) {
            //    if (aggInfo.getGroupingExprs().get(i).unwrapSlotRef(true) == null) continue;
            //    groupBySlots.add(aggInfo.getOutputTupleDesc().getSlots().get(i).getId());
            // }
            // ArrayList<Expr> bindingPredicates =
            //         analyzer.getBoundPredicates(tupleIds.get(0), groupBySlots, true);
            ArrayList<Expr> bindingPredicates = Lists.newArrayList();
            conjuncts.addAll(bindingPredicates);

            // also add remaining unassigned conjuncts_
            assignConjuncts(analyzer);

            // TODO(zc)
            // analyzer.createEquivConjuncts(tupleIds_.get(0), conjuncts_, groupBySlots);
        }
        // TODO(zc)
        // conjuncts_ = orderConjunctsByCost(conjuncts_);

        // Compute the mem layout for both tuples here for simplicity.
        aggInfo.getOutputTupleDesc().computeStatAndMemLayout();
        aggInfo.getIntermediateTupleDesc().computeStatAndMemLayout();

        // do this at the end so it can take all conjuncts into account
        computeStats(analyzer);

        // don't call createDefaultSMap(), it would point our conjuncts (= Having clause)
        // to our input; our conjuncts don't get substituted because they already
        // refer to our output
        outputSmap = getCombinedChildSmap();
        if (aggInfo.isMerge()) {
            aggInfo.substitute(aggInfo.getIntermediateSmap(), analyzer);
        }
        aggInfo.substitute(outputSmap, analyzer);

        // assert consistent aggregate expr and slot materialization
        // aggInfo.checkConsistency();
    }

    @Override
    public void computeStats(Analyzer analyzer) throws UserException {
        super.computeStats(analyzer);
        if (!analyzer.safeIsEnableJoinReorderBasedCost()) {
            return;
        }

        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = (long) statsDeriveResult.getRowCount();
    }

    @Override
    protected void computeOldCardinality() {
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        cardinality = 1;
        // cardinality: product of # of distinct values produced by grouping exprs
        for (Expr groupingExpr : groupingExprs) {
            long numDistinct = groupingExpr.getNumDistinctValues();
            // TODO: remove these before 1.0
            if (LOG.isDebugEnabled()) {
                LOG.debug("grouping expr: " + groupingExpr.toSql() + " #distinct=" + Long.toString(
                        numDistinct));
            }
            if (numDistinct == -1) {
                cardinality = -1;
                break;
            }
            cardinality *= numDistinct;
        }
        // take HAVING predicate into account
        if (LOG.isDebugEnabled()) {
            LOG.debug("Agg: cardinality=" + Long.toString(cardinality));
        }
        if (cardinality > 0) {
            cardinality = Math.round((double) cardinality * computeOldSelectivity());
            if (LOG.isDebugEnabled()) {
                LOG.debug("sel=" + Double.toString(computeOldSelectivity()));
            }
        }
        // if we ended up with an overflow, the estimate is certain to be wrong
        if (cardinality < 0) {
            cardinality = -1;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("stats Agg: cardinality=" + Long.toString(cardinality));
        }
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
                aggInfo.getIntermediateTupleId().asInt(),
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

    @Override
    public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
        super.getMaterializedIds(analyzer, ids);

        // we indirectly reference all grouping slots (because we write them)
        // so they're all materialized.
        aggInfo.getRefdSlots(ids);
    }

    @Override
    public Set<SlotId> computeInputSlotIds(Analyzer analyzer) throws NotImplementedException {
        Set<SlotId> result = Sets.newHashSet();
        // compute group by slot
        ArrayList<Expr> groupingExprs = aggInfo.getGroupingExprs();
        List<SlotId> groupingSlotIds = Lists.newArrayList();
        Expr.getIds(groupingExprs, null, groupingSlotIds);
        result.addAll(groupingSlotIds);

        // compute agg function slot
        ArrayList<FunctionCallExpr> aggregateExprs = aggInfo.getAggregateExprs();
        List<SlotId> aggregateSlotIds = Lists.newArrayList();
        Expr.getIds(aggregateExprs, null, aggregateSlotIds);
        result.addAll(aggregateSlotIds);

        // case: select count(*) from test
        // result is empty
        // Actually need to take a column as the input column of the agg operator
        if (result.isEmpty()) {
            TupleDescriptor tupleDesc = analyzer.getTupleDesc(getChild(0).getOutputTupleIds().get(0));
            // If the query result is empty set such as: select count(*) from table where 1=2
            // then the materialized slot will be empty
            // So the result should be empty also.
            if (!tupleDesc.getMaterializedSlots().isEmpty()) {
                result.add(tupleDesc.getMaterializedSlots().get(0).getId());
            }
        } else {
            // if some input slot for aggregate slot which is not materialized, we need to remove it from the result
            TupleDescriptor tupleDescriptor = aggInfo.getOutputTupleDesc();
            ArrayList<SlotDescriptor> slots = tupleDescriptor.getSlots();
            Set<SlotId> allUnRequestIds = Sets.newHashSet();
            Set<SlotId> allRequestIds = Sets.newHashSet();
            for (SlotDescriptor slot : slots) {
                if (!slot.isMaterialized()) {
                    List<SlotId> unRequestIds = Lists.newArrayList();
                    Expr.getIds(slot.getSourceExprs(), null, unRequestIds);
                    allUnRequestIds.addAll(unRequestIds);
                } else {
                    List<SlotId> requestIds = Lists.newArrayList();
                    Expr.getIds(slot.getSourceExprs(), null, requestIds);
                    allRequestIds.addAll(requestIds);
                }
            }
            allRequestIds.forEach(allUnRequestIds::remove);
            groupingSlotIds.forEach(allUnRequestIds::remove);
            allUnRequestIds.forEach(result::remove);
        }
        return result;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        super.finalize(analyzer);
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        for (int i = 0; i < groupingExprs.size(); i++) {
            aggInfo.getOutputTupleDesc().getSlots().get(i).setIsNullable(groupingExprs.get(i).isNullable());
            aggInfo.getIntermediateTupleDesc().getSlots().get(i).setIsNullable(groupingExprs.get(i).isNullable());
            aggInfo.getOutputTupleDesc().computeMemLayout();
        }
    }

    public void setColocate(boolean colocate) {
        isColocate = colocate;
    }


    public boolean isSortByGroupKey() {
        return sortByGroupKey != null;
    }

    public void setSortByGroupKey(SortInfo sortByGroupKey) {
        this.sortByGroupKey = sortByGroupKey;
    }
}
