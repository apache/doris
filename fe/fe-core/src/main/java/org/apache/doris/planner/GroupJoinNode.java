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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.thrift.TEqJoinCondition;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TGroupJoinAggFunction;
import org.apache.doris.thrift.TGroupJoinAggOutputMode;
import org.apache.doris.thrift.TGroupJoinAggSide;
import org.apache.doris.thrift.TGroupJoinNode;
import org.apache.doris.thrift.TJoinDistributionType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * GroupJoin operator that fuses a hash join with a hash aggregation.
 * <p>
 * The hash table is shared between join probing and aggregation state storage.
 * Because the join shuffle already distributes data by the join/group key,
 * only single-stage aggregation (FINAL_RESULT) is needed.
 */
public class GroupJoinNode extends PlanNode {

    private JoinOperator joinOp;
    private final List<BinaryPredicate> eqJoinConjuncts = Lists.newArrayList();
    private List<Expr> groupingExprs;
    private List<Expr> aggregateFunctions;
    private List<TGroupJoinAggSide> aggSides;
    private TGroupJoinAggOutputMode aggOutputMode;
    // Tuple that BE materializes the raw group-join result into (group keys + aggregate
    // values). It is distinct from PlanNode.outputTupleDesc: the latter is reused by the
    // generic projection attach (final projections above the fused aggregate) and must not
    // shadow this node's own materialization tuple (see getOutputTupleIds/toThrift).
    private TupleDescriptor materializedTupleDesc;
    private DistributionMode distrMode;
    private boolean isColocate = false;

    // Intermediate tuple descriptors for left and right child outputs
    private List<TupleDescriptor> vIntermediateTupleDescList = Lists.newArrayList();
    private final Set<SlotId> hashOutputSlotIds = Sets.newHashSet();

    public GroupJoinNode(PlanNodeId id, PlanNode leftChild, PlanNode rightChild) {
        super(id, "GROUP JOIN");
        // Add both children's output tuple ids for row_tuples thrift field
        tupleIds.addAll(leftChild.getOutputTupleIds());
        tupleIds.addAll(rightChild.getOutputTupleIds());
        children.add(leftChild);
        children.add(rightChild);
    }

    /**
     * Tuples a parent node consumes from this node.
     * <p>
     * When the generic final-projection attach hung a projection on this node (see
     * setMaterializedTupleDesc), what the parent sees is the post-projection tuple in
     * PlanNode.outputTupleDesc, not the raw tuple the BE operator materializes into:
     * attaching a projection re-binds the projected expressions' ExprIds to the slots of the
     * projection tuple (PlanTranslatorContext.createSlotDesc), so every expression a parent
     * later translates refers to that tuple. Returning materializedTupleDesc here made those
     * parents describe their input with the pre-projection tuple, and the BE rejected the
     * plan with "VSlotRef have invalid slot id" (e.g. a SelectNode inserted above a fused
     * GROUP JOIN that already carries projections). Without an attached projection the
     * materialized tuple is exactly what this node emits.
     */
    @Override
    public ArrayList<TupleId> getOutputTupleIds() {
        if (outputTupleDesc != null) {
            return Lists.newArrayList(outputTupleDesc.getId());
        }
        if (materializedTupleDesc != null) {
            return Lists.newArrayList(materializedTupleDesc.getId());
        }
        return tupleIds;
    }

    public void setJoinOp(JoinOperator joinOp) {
        this.joinOp = joinOp;
    }

    public JoinOperator getJoinOp() {
        return joinOp;
    }

    public void addEqJoinConjunct(BinaryPredicate conjunct) {
        eqJoinConjuncts.add(conjunct);
    }

    public List<BinaryPredicate> getEqJoinConjuncts() {
        return eqJoinConjuncts;
    }

    public void setGroupingExprs(List<Expr> groupingExprs) {
        this.groupingExprs = groupingExprs;
    }

    public List<Expr> getGroupingExprs() {
        return groupingExprs;
    }

    public void setAggregateFunctions(List<Expr> aggregateFunctions) {
        this.aggregateFunctions = aggregateFunctions;
    }

    public List<Expr> getAggregateFunctions() {
        return aggregateFunctions;
    }

    public void setAggSides(List<TGroupJoinAggSide> aggSides) {
        this.aggSides = aggSides;
    }

    public List<TGroupJoinAggSide> getAggSides() {
        return aggSides;
    }

    public void setAggOutputMode(TGroupJoinAggOutputMode aggOutputMode) {
        this.aggOutputMode = aggOutputMode;
    }

    public TGroupJoinAggOutputMode getAggOutputMode() {
        return aggOutputMode;
    }

    /**
     * Set the tuple the BE group-join operator materializes its raw rows (group keys +
     * aggregate values) into. Must be called before toThrift.
     * <p>
     * Note: this is intentionally NOT an override of PlanNode.setOutputTupleDesc. The
     * generic final-projection attach (PhysicalPlanTranslator.visitPhysicalProject) writes
     * PlanNode.outputTupleDesc for the projections above the fused aggregate; that field is
     * read by PlanNode.getExplainString and PlanNode.treeToThriftHelper (top-level
     * output_tuple_id). Keeping the materialization tuple separate mirrors AggregationNode,
     * whose raw result tuple lives in AggregateInfo while PlanNode.outputTupleDesc holds the
     * final projected tuple. Mixing the two (as a shadowing field did) made EXPLAIN NPE and
     * made TPlanNode miss its output_tuple_id when projections were attached.
     */
    public void setMaterializedTupleDesc(TupleDescriptor materializedTupleDesc) {
        this.materializedTupleDesc = materializedTupleDesc;
        if (materializedTupleDesc != null) {
            tupleIds.add(materializedTupleDesc.getId());
        }
    }

    public void setDistributionMode(DistributionMode distrMode) {
        this.distrMode = distrMode;
    }

    public DistributionMode getDistributionMode() {
        return distrMode;
    }

    public void setColocate(boolean colocate) {
        this.isColocate = colocate;
    }

    public boolean isColocate() {
        return isColocate;
    }

    @Override
    public boolean requiresShuffleForCorrectness() {
        return distrMode == DistributionMode.PARTITIONED
                || distrMode == DistributionMode.BUCKET_SHUFFLE
                || isColocate;
    }

    @Override
    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(
            PlanTranslatorContext translatorContext, PlanNode parent, LocalExchangeTypeRequire parentRequire) {
        LocalExchangeTypeRequire require;
        if (isColocate || distrMode == DistributionMode.BUCKET_SHUFFLE) {
            require = LocalExchangeTypeRequire.requireBucketHash();
        } else {
            // Serial exchanges collapse the global instance mapping. Repartition both
            // inputs locally, as in the non-broadcast HashJoin path.
            boolean serialSource = fragment != null
                    && fragment.useSerialSource(translatorContext.getConnectContext());
            require = serialSource ? LocalExchangeTypeRequire.requireHash()
                    : LocalExchangeTypeRequire.requireGlobalExecutionHash();
        }
        Pair<PlanNode, LocalExchangeType> probeResult = enforceRequire(
                translatorContext, children.get(0), 0, require);
        Pair<PlanNode, LocalExchangeType> buildResult = enforceRequire(
                translatorContext, children.get(1), 1, require);
        children = Lists.newArrayList(probeResult.first, buildResult.first);
        return Pair.of(this, probeResult.second);
    }

    @Override
    protected boolean shouldResetSerialFlagForChild(int childIndex) {
        return childIndex == 1;
    }

    public void setvIntermediateTupleDescList(List<TupleDescriptor> vIntermediateTupleDescList) {
        this.vIntermediateTupleDescList = vIntermediateTupleDescList;
    }

    public List<TupleDescriptor> getvIntermediateTupleDescList() {
        return vIntermediateTupleDescList;
    }

    public void addSlotIdToHashOutputSlotIds(SlotId slotId) {
        hashOutputSlotIds.add(slotId);
    }

    public Set<SlotId> getHashOutputSlotIds() {
        return hashOutputSlotIds;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.GROUP_JOIN_NODE;
        msg.group_join_node = new TGroupJoinNode();

        // Join info
        msg.group_join_node.join_op = joinOp.toThrift();
        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            TEqJoinCondition eqJoinCondition = new TEqJoinCondition(
                    ExprToThriftVisitor.treeToThrift(eqJoinPredicate.getChild(0)),
                    ExprToThriftVisitor.treeToThrift(eqJoinPredicate.getChild(1)));
            eqJoinCondition.setOpcode(ExprToThriftVisitor.toThriftOpcode(eqJoinPredicate.getOp()));
            msg.group_join_node.addToEqJoinConjuncts(eqJoinCondition);
        }
        msg.group_join_node.setDistType(isColocate
                ? TJoinDistributionType.COLOCATE : distrMode != null ? distrMode.toThrift()
                : TJoinDistributionType.PARTITIONED);

        // Aggregation info
        for (Expr groupingExpr : groupingExprs) {
            msg.group_join_node.addToGroupingExprs(ExprToThriftVisitor.treeToThrift(groupingExpr));
        }
        // Build aggregate functions with side annotation
        Preconditions.checkState(aggregateFunctions.size() == aggSides.size(),
                "aggregateFunctions and aggSides must have same size");
        for (int i = 0; i < aggregateFunctions.size(); i++) {
            TGroupJoinAggFunction aggFunc = new TGroupJoinAggFunction();
            aggFunc.setAggregateFunction(ExprToThriftVisitor.treeToThrift(aggregateFunctions.get(i)));
            aggFunc.setInputSide(aggSides.get(i));
            msg.group_join_node.addToAggregateFunctions(aggFunc);
        }
        msg.group_join_node.setAggOutputMode(aggOutputMode);
        // BE always materializes into this tuple (groupjoin_probe_operator reads
        // group_join_node.output_tuple_id), so it must be set.
        Preconditions.checkState(materializedTupleDesc != null,
                "materializedTupleDesc must be set before GroupJoinNode.toThrift");
        msg.group_join_node.setOutputTupleId(materializedTupleDesc.getId().asInt());
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(detailPrefix).append("group join: ");
        output.append(joinOp.toString()).append("\n");

        output.append(detailPrefix).append("  join op: ").append(joinOp).append("\n");
        output.append(detailPrefix).append("  equi conjuncts: ").append(eqJoinConjuncts).append("\n");
        output.append(detailPrefix).append("  grouping exprs: ").append(groupingExprs).append("\n");
        output.append(detailPrefix).append("  agg functions: ").append(aggregateFunctions).append("\n");
        output.append(detailPrefix).append("  distribution: ")
                .append(isColocate ? "COLOCATE" : distrMode).append("\n");

        return output.toString();
    }

    @Override
    public int getNumInstances() {
        // Same as HashJoin: if colocate or broadcast, use right child's num instances
        return Math.max(children.get(0).getNumInstances(), children.get(1).getNumInstances());
    }

    @Override
    public ArrayList<TupleId> getTupleIds() {
        ArrayList<TupleId> tupleIds = Lists.newArrayList();
        tupleIds.addAll(super.getTupleIds());
        if (materializedTupleDesc != null) {
            tupleIds.add(materializedTupleDesc.getId());
        }
        return tupleIds;
    }
}
