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

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.thrift.TAggregationNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Aggregation computation.
 */
public class AggregationNode extends PlanNode {
    private final static Logger LOG = LogManager.getLogger(AggregationNode.class);
    private final AggregateInfo aggInfo;

    // Set to true if this aggregation node needs to run the Finalize step. This
    // node is the root node of a distributed aggregation.
    private boolean needsFinalize;

    // If true, use streaming preaggregation algorithm. Not valid if this is a merge agg.
    private boolean useStreamingPreagg;

    /**
     * Create an agg node that is not an intermediate node.
     * isIntermediate is true if it is a slave node in a 2-part agg plan.
     */
    public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
        super(id, aggInfo.getOutputTupleId().asList(), "AGGREGATE");
        this.aggInfo = aggInfo;
        this.children.add(input);
        this.needsFinalize = true;
        updateplanNodeName();
    }

    /**
     * Copy c'tor used in clone().
     */
    private AggregationNode(PlanNodeId id, AggregationNode src) {
        super(id, src, "AGGREGATE");
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
    public void setIsPreagg(PlannerContext ctx_) {
        useStreamingPreagg =  ctx_.getQueryOptions().isSetDisableStreamPreaggregations()
                && !ctx_.getQueryOptions().disable_stream_preaggregations
                && aggInfo.getGroupingExprs().size() > 0;
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
        aggInfo.substitute(outputSmap, analyzer);

        // assert consistent aggregate expr and slot materialization
        // aggInfo.checkConsistency();
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        if (!analyzer.safeIsEnableJoinReorderBasedCost()) {
            return;
        }
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        cardinality = 1;
        // cardinality: product of # of distinct values produced by grouping exprs
        for (Expr groupingExpr : groupingExprs) {
            long numDistinct = groupingExpr.getNumDistinctValues();
            LOG.debug("grouping expr: " + groupingExpr.toSql() + " #distinct=" + Long.toString(
                    numDistinct));
            if (numDistinct == -1) {
                cardinality = -1;
                break;
            }
            // This is prone to overflow, because we keep multiplying cardinalities,
            // even if the grouping exprs are functionally dependent (example:
            // group by the primary key of a table plus a number of other columns from that
            // same table)
            // TODO: try to recognize functional dependencies
            // TODO: as a shortcut, instead of recognizing functional dependencies,
            // limit the contribution of a single table to the number of rows
            // of that table (so that when we're grouping by the primary key col plus
            // some others, the estimate doesn't overshoot dramatically)
            cardinality *= numDistinct;
        }
        if (cardinality > 0) {
            LOG.debug("sel=" + Double.toString(computeSelectivity()));
            applyConjunctsSelectivity();
        }
        // if we ended up with an overflow, the estimate is certain to be wrong
        if (cardinality < 0) {
            cardinality = -1;
        }

        capCardinalityAtLimit();
        if (LOG.isDebugEnabled()) {
            LOG.debug("stats Agg: cardinality={}", cardinality);
        }
    }

    @Override
    protected void computeOldCardinality() {
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        cardinality = 1;
        // cardinality: product of # of distinct values produced by grouping exprs
        for (Expr groupingExpr : groupingExprs) {
            long numDistinct = groupingExpr.getNumDistinctValues();
            // TODO: remove these before 1.0
            LOG.debug("grouping expr: " + groupingExpr.toSql() + " #distinct=" + Long.toString(
                    numDistinct));
            if (numDistinct == -1) {
                cardinality = -1;
                break;
            }
            cardinality *= numDistinct;
        }
        // take HAVING predicate into account
        LOG.debug("Agg: cardinality=" + Long.toString(cardinality));
        if (cardinality > 0) {
            cardinality = Math.round((double) cardinality * computeOldSelectivity());
            LOG.debug("sel=" + Double.toString(computeOldSelectivity()));
        }
        // if we ended up with an overflow, the estimate is certain to be wrong
        if (cardinality < 0) {
            cardinality = -1;
        }
        LOG.debug("stats Agg: cardinality=" + Long.toString(cardinality));
    }

    private void updateplanNodeName() {
        StringBuilder sb = new StringBuilder();
        sb.append(VectorizedUtil.isVectorized() ? "VAGGREGATE" : "AGGREGATE");
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
        msg.node_type = TPlanNodeType.AGGREGATION_NODE;
        List<TExpr> aggregateFunctions = Lists.newArrayList();
        // only serialize agg exprs that are being materialized
        for (FunctionCallExpr e : aggInfo.getMaterializedAggregateExprs()) {
            aggregateFunctions.add(e.treeToThrift());
        }
        msg.agg_node =
                new TAggregationNode(aggregateFunctions, aggInfo.getIntermediateTupleId().asInt(),
                        aggInfo.getOutputTupleId().asInt(), needsFinalize);
        msg.agg_node.setUseStreamingPreaggregation(useStreamingPreagg);
        msg.agg_node.setIsUpdateStage(!aggInfo.isMerge());
        msg.agg_node.setAggregateFunctionChangedFlags(aggInfo.getMaterializedAggregateExprChangedFlags());
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
        StringBuilder output = new StringBuilder();
        String nameDetail = getDisplayLabelDetail();
        if (nameDetail != null) {
            output.append(detailPrefix + nameDetail + "\n");
        }

        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }

        if (aggInfo.getAggregateExprs() != null && aggInfo.getMaterializedAggregateExprs().size() > 0) {
            output.append(detailPrefix + "output: ").append(
                    getExplainString(aggInfo.getAggregateExprs()) + "\n");
        }
        // TODO: group by can be very long. Break it into multiple lines
        output.append(detailPrefix + "group by: ").append(
          getExplainString(aggInfo.getGroupingExprs()) + "\n");
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix + "having: ").append(getExplainString(conjuncts) + "\n");
        }
        output.append(detailPrefix).append(String.format(
                "cardinality=%s", cardinality)).append("\n");
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
    public int getNumInstances() {
        return children.get(0).getNumInstances();
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
        }
        return result;
    }
}
