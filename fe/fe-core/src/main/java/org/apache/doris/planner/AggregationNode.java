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
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.qe.dict.IDict;
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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Aggregation computation.
 */
public class AggregationNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(AggregationNode.class);
    private final AggregateInfo aggInfo;

    // Set to true if this aggregation node needs to run the Finalize step. This
    // node is the root node of a distributed aggregation.
    private boolean needsFinalize;

    // If true, use streaming preaggregation algorithm. Not valid if this is a merge agg.
    private boolean useStreamingPreagg;

    private static Set<String> dictAggregationSupportedFunction = Sets.newHashSet(FunctionSet.COUNT);

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

        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = statsDeriveResult.getRowCount();
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
            new TAggregationNode(
                aggregateFunctions,
                aggInfo.getIntermediateTupleId().asInt(),
                aggInfo.getOutputTupleId().asInt(), needsFinalize);
        msg.agg_node.setUseStreamingPreaggregation(useStreamingPreagg);
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
    public Set<SlotId> computeInputSlotIds() throws NotImplementedException {
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
        return result;
    }

    @Override
    public void filterDictSlot(DecodeContext context) {
        Set<Integer> disabledDictOptimizationSlotIdSet = context.getDictOptimizationDisabledSlot();
        List<Expr> groupingExpr = aggInfo.getGroupingExprs();
        if (groupingExpr != null) {
            groupingExpr.forEach(e -> {
                if (!(e instanceof SlotRef)) {
                    SlotId.getAllSlotIdFromExpr(e, disabledDictOptimizationSlotIdSet);
                }
            });
        }
        List<FunctionCallExpr> aggExprList = aggInfo.getAggregateExprs();
        if (aggExprList != null) {
            aggExprList.forEach(e -> {
                    if (!dictAggregationSupportedFunction.contains(e.getFnName().getFunction())) {
                        SlotId.getAllSlotIdFromExpr(e, disabledDictOptimizationSlotIdSet);
                    }
                }
            );
        }

        findEncodeNeedSlot(context);
        super.filterDictSlot(context);
    }

    @Override
    public void updateSlots(DecodeContext context) {
        if (this.aggInfo.getAggPhase_().equals(AggregateInfo.AggPhase.SECOND)) {
            AggregationNode child = (AggregationNode) getChild(0);
            AggregateInfo aggInfo = context.getSecondPhaseAggInfo(child);
            this.originTupleIds = tupleIds;
            this.aggInfo = aggInfo;
            this.tupleIds = aggInfo.getOutputTupleId().asList();
            tupleIds.stream()
                .map(id -> context.getTableDesc().getTupleDesc(id))
                    .map(TupleDescriptor::getSlots)
                        .flatMap(Collection::stream)
                            .forEach(s -> s.setIsMaterialized(true));
            aggInfo.materializeRequiredSlots(context.getAnalyzer(), null);
        } else {
            for (SlotRef slotRef : requireEncodeSlotRefList) {
                if (context.slotNeedEncode(slotRef.getSlotId().asInt())) {
                    context.updateSlotRefType(slotRef);
                }
            }

            try {
                ArrayList<Integer> materializedSlots = aggInfo.getMaterializedSlots_();
                originTupleIds = tupleIds;
                ArrayList<FunctionCallExpr> allAggExprs = aggInfo.getAggregateExprs();
                ArrayList<FunctionCallExpr> distinctAggExprs = aggInfo.getDistinctAggExprs_();
                ArrayList<Expr> grouppingExprs = aggInfo.getGroupingExprs();
                PlanNode planNode = getChild(0);
                if (planNode.getOriginTupleIds() != null) {
                    ArrayList<TupleId> childNewTupleIds = planNode.getTupleIds();
                    ArrayList<TupleId> childOldTupleIds = planNode.getOriginTupleIds();
                    DictPlanner.exprUpdate(childNewTupleIds,
                        childOldTupleIds,
                        allAggExprs,
                        context);
                    DictPlanner.exprUpdate(planNode.getTupleIds(),
                        childOldTupleIds,
                        grouppingExprs,
                        context);
                    DictPlanner.exprUpdate(planNode.getTupleIds(),
                        childOldTupleIds,
                        distinctAggExprs,
                        context);
                }
                if (distinctAggExprs != null) {
                    allAggExprs.addAll(distinctAggExprs);
                    if (distinctAggExprs.get(0).getFnName().getFunction().equalsIgnoreCase("group_concat")) {
                        grouppingExprs.remove(distinctAggExprs.get(0).getChild(0).ignoreImplicitCast());
                    } else {
                        for (Expr expr : distinctAggExprs.get(0).getChildren()) {
                            grouppingExprs.remove(expr.ignoreImplicitCast());
                        }
                    }
                    grouppingExprs.removeAll(distinctAggExprs);
                }
                AggregateInfo origin = aggInfo;
                aggInfo = AggregateInfo.create(
                    aggInfo.getGroupingExprs(),
                    allAggExprs,
                    null,
                    context.getAnalyzer());
                tupleIds = aggInfo.getOutputTupleId().asList();
                aggInfo.setMaterializedSlots_(materializedSlots);
                aggInfo.getMergeAggInfo().setMaterializedSlots_(materializedSlots);
                if (aggInfo.isDistinctAgg()) {
                    AggregateInfo secondPhaseAggInfo =  aggInfo.getSecondPhaseDistinctAggInfo();
                    AggregateInfo originMergeAggInfoOfSecondPhaseAgg = origin.getSecondPhaseDistinctAggInfo().getMergeAggInfo();
                    secondPhaseAggInfo.setMaterializedSlots_(origin.getSecondPhaseDistinctAggInfo().getMaterializedSlots_());
                    secondPhaseAggInfo.getMergeAggInfo().setMaterializedSlots_(originMergeAggInfoOfSecondPhaseAgg.getMaterializedSlots_());
                    context.putNewFatherAgg(this, secondPhaseAggInfo);
                }
                TupleDescriptor newOutputTupleDesc = aggInfo.getOutputTupleDesc();
                updateOutputSlots(newOutputTupleDesc, context);
                // below logic is necessary for the DecodeNode generation,
                // we generate DecodeNode when the output slot have intersection
                // with the keySet of DecodeContext::dictSlotIdToColumnDict
                for (SlotDescriptor slotDescriptor : newOutputTupleDesc.getSlots()) {
                    slotDescriptor.setIsMaterialized(true);
                    for (Expr expr: slotDescriptor.getSourceExprs()) {
                        if (expr instanceof SlotRef) {
                            SlotRef slotRef = (SlotRef) expr;
                            int slotId = slotRef.getSlotId().asInt();
                            IDict columnDict = context.getColumnDictByDictSlotId(slotId);
                            if (columnDict != null) {
                                context.addAvailableDict(slotDescriptor.getId().asInt(), columnDict);
                            }
                        }
                    }
                }
            } catch (AnalysisException e) {
                throw new RuntimeException("Failed to create new AggInfo", e);
            }
        }
        DictPlanner.exprUpdate(tupleIds, originTupleIds, conjuncts, context);
    }

    private void updateOutputSlots(TupleDescriptor newTupleDesc, DecodeContext context) {
        if (outputSlotIds == null) {
            return;
        }
        DescriptorTable tableDesc = context.getTableDesc();
        List<SlotId> newOutputSlotIds = new ArrayList<>();
        List<SlotDescriptor> slotDescriptorList = newTupleDesc.getSlots();
        for (SlotId slotId: outputSlotIds) {
            SlotDescriptor slotDesc = tableDesc.getSlotDesc(slotId);
            int slotDescOffset = slotDesc.getSlotOffset();
            newOutputSlotIds.add(slotDescriptorList.get(slotDescOffset).getId());
        }
        outputSlotIds = newOutputSlotIds;
    }

    private void findEncodeNeedSlot(DecodeContext context) {
        findEncodeNeedSlot(context, aggInfo);
        AggregateInfo mergeAggInfo = aggInfo.getMergeAggInfo();
        if (mergeAggInfo != null) {
            findEncodeNeedSlot(context, mergeAggInfo);
        }
        AggregateInfo secondPhaseInfo = aggInfo.getSecondPhaseDistinctAggInfo();
        if (secondPhaseInfo != null) {
            findEncodeNeedSlot(context, secondPhaseInfo);
        }
    }

    private void findEncodeNeedSlot(DecodeContext context, AggregateInfo aggInfo) {
        findEncodeNeedSlot(aggInfo.getGroupingExprs(), context);
        for (FunctionCallExpr func : aggInfo.getAggregateExprs()) {
            String funcName = func.getFnName().getFunction();
            if (!dictAggregationSupportedFunction.contains(funcName)) {
                continue;
            }
            FunctionParams functionParams = func.getParams();
            List<Expr> funcParamExprList = functionParams.exprs();
            if (funcParamExprList == null) {
                continue;
            }
            findEncodeNeedSlot(funcParamExprList, context);
        }
    }

    private void findEncodeNeedSlot(List<Expr> exprList, DecodeContext context) {
        for (Expr expr : exprList) {
            if (expr instanceof SlotRef) {
                Queue<SlotRef> slotRefQueue = new LinkedList<>();
                slotRefQueue.add((SlotRef) expr);
                while (!slotRefQueue.isEmpty()) {
                    SlotRef slotRef = slotRefQueue.poll();
                    Column column = slotRef.getColumn();
                    // means it's a colRef
                    if (column != null) {
                        int slotId = slotRef.getSlotId().asInt();
                        IDict columnDict = context.getColumnDictBySlotId(slotId);
                        if (columnDict != null && !context.dictOptForbiddenForSlot(slotId)) {
                            requireEncodeSlotToDictColumn.put(slotRef, columnDict);
                            requireEncodeSlotRefList.add(slotRef);
                            context.addEncodeNeededSlot(slotId);
                        }
                        continue;
                    }
                    SlotDescriptor slotDesc = slotRef.getDesc();
                    slotDesc.getSourceExprs()
                        .stream()
                        .filter(e -> e instanceof SlotRef
                        ).forEach(e -> slotRefQueue.add((SlotRef) e));
                }
            }
        }
    }

    @Override
    public void generateDecodeNode(DecodeContext decodeContext) {
        Set<Integer> originSlotIdSet = decodeContext.getDictCodableSlot();
        List<Integer> originSlotInOutput = null;
        if (outputSlotIds != null) {
            // TODO: should handle all the expr type instead of slotRef only
            originSlotInOutput = outputSlotIds
                .stream().map(SlotId::asInt)
                .filter(originSlotIdSet::contains)
                .collect(Collectors.toList());
        } else {
            originSlotInOutput =
                    aggInfo.getOutputTupleDesc().getSlots()
                    .stream()
                    .map(d -> d.getId().asInt())
                    .filter(originSlotIdSet::contains)
                    .collect(Collectors.toList());

        }
        if (originSlotInOutput.isEmpty()) {
            return;
        }
        if (!aggInfo.isDistinctAgg()) {
            decodeContext.newDecodeNode(this, originSlotInOutput, originTupleIds);
        }
    }

    @Override
    public void initOutputSlotIds(Set<SlotId> requiredSlotIdSet, Analyzer analyzer) throws NotImplementedException {
        outputSlotIds = Lists.newArrayList();
        for (TupleId tupleId : tupleIds) {
            for (SlotDescriptor slotDescriptor : analyzer.getTupleDesc(tupleId).getSlots()) {
                if (slotDescriptor.isMaterialized() &&
                    (requiredSlotIdSet == null || requiredSlotIdSet.contains(slotDescriptor.getId()))) {
                    outputSlotIds.add(slotDescriptor.getId());
                }
            }
        }
    }
}
