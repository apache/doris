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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/HashJoinNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.thrift.TEqJoinCondition;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.THashJoinNode;
import org.apache.doris.thrift.TJoinDistributionType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hash join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 */
public class HashJoinNode extends JoinNodeBase {

    // predicates of the form 'a=b' or 'a<=>b'
    private final List<BinaryPredicate> eqJoinConjuncts = Lists.newArrayList();
    // join conjuncts from the JOIN clause that aren't equi-join predicates
    private List<Expr> otherJoinConjuncts;

    private List<Expr> markJoinConjuncts;

    private DistributionMode distrMode;
    private boolean isColocate = false; //the flag for colocate join
    private String colocateReason = ""; // if can not do colocate join, set reason here

    private final Set<SlotId> hashOutputSlotIds = Sets.newHashSet(); //init for nereids

    // TODO: need review
    private final Map<ExprId, SlotId> hashOutputExprSlotIdMap = Maps.newHashMap();

    private final Expr matchCondition;

    public HashJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, JoinOperator joinOp,
            List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts, Expr matchCondition,
            List<Expr> markJoinConjuncts, boolean isMarkJoin) {
        super(id, "HASH JOIN", joinOp, isMarkJoin);
        Preconditions.checkArgument((eqJoinConjuncts != null && !eqJoinConjuncts.isEmpty())
                || (markJoinConjuncts != null && !markJoinConjuncts.isEmpty()));
        Preconditions.checkArgument(otherJoinConjuncts != null);

        if (joinOp.equals(JoinOperator.LEFT_ANTI_JOIN) || joinOp.equals(JoinOperator.LEFT_SEMI_JOIN)
                || joinOp.equals(JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN)) {
            tupleIds.addAll(outer.getOutputTupleIds());
        } else if (joinOp.equals(JoinOperator.RIGHT_ANTI_JOIN) || joinOp.equals(JoinOperator.RIGHT_SEMI_JOIN)) {
            tupleIds.addAll(inner.getOutputTupleIds());
        } else {
            tupleIds.addAll(outer.getOutputTupleIds());
            tupleIds.addAll(inner.getOutputTupleIds());
        }

        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            Preconditions.checkArgument(eqJoinPredicate instanceof BinaryPredicate);
            BinaryPredicate eqJoin = (BinaryPredicate) eqJoinPredicate;
            this.eqJoinConjuncts.add(eqJoin);
        }
        this.distrMode = DistributionMode.NONE;
        this.otherJoinConjuncts = otherJoinConjuncts;
        this.matchCondition = matchCondition;
        this.markJoinConjuncts = markJoinConjuncts;
        children.add(outer);
        children.add(inner);
        this.vIntermediateTupleDescList = Lists.newArrayList();
        this.outputTupleDesc = null;
    }

    public List<BinaryPredicate> getEqJoinConjuncts() {
        return eqJoinConjuncts;
    }

    public DistributionMode getDistributionMode() {
        return distrMode;
    }

    public void setDistributionMode(DistributionMode distrMode) {
        this.distrMode = distrMode;
    }

    public boolean isBucketShuffle() {
        return distrMode.equals(DistributionMode.BUCKET_SHUFFLE);
    }

    public void setColocate(boolean colocate, String reason) {
        isColocate = colocate;
        colocateReason = reason;
    }

    public boolean isColocate() {
        return isColocate;
    }

    @Override
    public boolean requiresShuffleForCorrectness() {
        // BE: HashJoinBuild/Probe.is_shuffled_operator() = PARTITIONED || BUCKET_SHUFFLE || COLOCATE.
        // (BROADCAST and NONE are not shuffled — they don't depend on hash distribution.)
        return distrMode == DistributionMode.PARTITIONED
                || distrMode == DistributionMode.BUCKET_SHUFFLE
                || isColocate;
    }

    public Map<ExprId, SlotId> getHashOutputExprSlotIdMap() {
        return hashOutputExprSlotIdMap;
    }

    public Set<SlotId> getHashOutputSlotIds() {
        return hashOutputSlotIds;
    }

    //nereids only
    public void addSlotIdToHashOutputSlotIds(SlotId slotId) {
        hashOutputSlotIds.add(slotId);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
        msg.hash_join_node = new THashJoinNode();
        msg.hash_join_node.join_op = joinOp.toThrift();
        msg.hash_join_node.setIsBroadcastJoin(distrMode == DistributionMode.BROADCAST);
        msg.hash_join_node.setIsMark(isMarkJoin());
        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            TEqJoinCondition eqJoinCondition = new TEqJoinCondition(
                    ExprToThriftVisitor.treeToThrift(eqJoinPredicate.getChild(0)),
                    ExprToThriftVisitor.treeToThrift(eqJoinPredicate.getChild(1)));
            eqJoinCondition.setOpcode(ExprToThriftVisitor.toThriftOpcode(eqJoinPredicate.getOp()));
            msg.hash_join_node.addToEqJoinConjuncts(eqJoinCondition);
        }
        for (Expr e : otherJoinConjuncts) {
            msg.hash_join_node.addToOtherJoinConjuncts(ExprToThriftVisitor.treeToThrift(e));
        }
        if (matchCondition != null) {
            Preconditions.checkState(joinOp == JoinOperator.ASOF_LEFT_OUTER_JOIN
                    || joinOp == JoinOperator.ASOF_LEFT_INNER_JOIN, "match condition is not allowed in " + joinOp);
            msg.hash_join_node.setMatchCondition(ExprToThriftVisitor.treeToThrift(matchCondition));
        }

        if (markJoinConjuncts != null) {
            if (eqJoinConjuncts.isEmpty()) {
                Preconditions.checkState(joinOp == JoinOperator.NULL_AWARE_LEFT_SEMI_JOIN
                        || joinOp == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN);
                // because eqJoinConjuncts mustn't be empty in thrift
                // we have to use markJoinConjuncts instead
                for (Expr e : markJoinConjuncts) {
                    Preconditions.checkState(e instanceof BinaryPredicate,
                            "mark join conjunct must be BinaryPredicate");
                    TEqJoinCondition eqJoinCondition = new TEqJoinCondition(
                            ExprToThriftVisitor.treeToThrift(e.getChild(0)),
                            ExprToThriftVisitor.treeToThrift(e.getChild(1)));
                    eqJoinCondition.setOpcode(ExprToThriftVisitor.toThriftOpcode(((BinaryPredicate) e).getOp()));
                    msg.hash_join_node.addToEqJoinConjuncts(eqJoinCondition);
                }
            } else {
                for (Expr e : markJoinConjuncts) {
                    msg.hash_join_node.addToMarkJoinConjuncts(ExprToThriftVisitor.treeToThrift(e));
                }
            }
        }

        if (hashOutputSlotIds != null) {
            for (SlotId slotId : hashOutputSlotIds) {
                msg.hash_join_node.addToHashOutputSlotIds(slotId.asInt());
            }
        }

        if (vIntermediateTupleDescList != null) {
            for (TupleDescriptor tupleDescriptor : vIntermediateTupleDescList) {
                msg.hash_join_node.addToVintermediateTupleIdList(tupleDescriptor.getId().asInt());
            }
        }
        msg.hash_join_node.setDistType(isColocate ? TJoinDistributionType.COLOCATE : distrMode.toThrift());
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        String distrModeStr = "";
        if (isColocate) {
            distrModeStr = "COLOCATE[" + colocateReason + "]";
        } else {
            distrModeStr = distrMode.toString();
        }
        StringBuilder output =
                new StringBuilder().append(detailPrefix).append("join op: ").append(joinOp.toString()).append("(")
                        .append(distrModeStr).append(")").append("[").append(colocateReason).append("]\n");
        if (detailLevel == TExplainLevel.BRIEF) {
            output.append(detailPrefix).append(
                    String.format("cardinality=%,d", cardinality)).append("\n");
            return output.toString();
        }

        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            output.append(detailPrefix).append("equal join conjunct: ")
                    .append(eqJoinPredicate.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)).append("\n");
        }
        if (!otherJoinConjuncts.isEmpty()) {
            output.append(detailPrefix).append("other join predicates: ")
                    .append(getExplainString(otherJoinConjuncts)).append("\n");
        }
        if (matchCondition != null) {
            output.append(detailPrefix).append("match condition: ")
                    .append(matchCondition.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)).append("\n");
        }
        if (markJoinConjuncts != null && !markJoinConjuncts.isEmpty()) {
            output.append(detailPrefix).append("mark join predicates: ")
                    .append(getExplainString(markJoinConjuncts)).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("other predicates: ").append(getExplainString(conjuncts)).append("\n");
        }

        output.append(detailPrefix).append(String.format("cardinality=%,d", cardinality)).append("\n");
        if (outputTupleDesc != null) {
            output.append(detailPrefix).append("vec output tuple id: ").append(outputTupleDesc.getId()).append("\n");
        }
        if (outputTupleDesc != null) {
            output.append(detailPrefix).append("output tuple id: ").append(outputTupleDesc.getId()).append("\n");
        }
        if (vIntermediateTupleDescList != null) {
            output.append(detailPrefix).append("vIntermediate tuple ids: ");
            for (TupleDescriptor tupleDescriptor : vIntermediateTupleDescList) {
                output.append(tupleDescriptor.getId()).append(" ");
            }
            output.append("\n");
        }
        if (outputSlotIds != null) {
            output.append(detailPrefix).append("output slot ids: ");
            for (SlotId slotId : outputSlotIds) {
                output.append(slotId).append(" ");
            }
            output.append("\n");
        }
        if (hashOutputSlotIds != null) {
            output.append(detailPrefix).append("hash output slot ids: ");
            for (SlotId slotId : hashOutputSlotIds) {
                output.append(slotId).append(" ");
            }
            output.append("\n");
        }
        if (detailLevel == TExplainLevel.VERBOSE) {
            output.append(detailPrefix).append("isMarkJoin: ").append(isMarkJoin()).append("\n");
        }
        return output.toString();
    }

    /**
     * Used by nereids.
     */
    public void setOtherJoinConjuncts(List<Expr> otherJoinConjuncts) {
        this.otherJoinConjuncts = otherJoinConjuncts;
    }

    public void setMarkJoinConjuncts(List<Expr> markJoinConjuncts) {
        this.markJoinConjuncts = markJoinConjuncts;
    }

    public List<Expr> getOtherJoinConjuncts() {
        return otherJoinConjuncts;
    }

    public List<Expr> getMarkJoinConjuncts() {
        return markJoinConjuncts;
    }

    @Override
    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(
            PlanTranslatorContext translatorContext, PlanNode parent, LocalExchangeTypeRequire parentRequire) {

        LocalExchangeTypeRequire probeSideRequire;
        LocalExchangeTypeRequire buildSideRequire;
        LocalExchangeType outputType = null;

        if (joinOp == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) {
            buildSideRequire = probeSideRequire = LocalExchangeTypeRequire.noRequire();
            outputType = LocalExchangeType.NOOP;
        } else if (distrMode == DistributionMode.BROADCAST) {
            // BE HashJoinProbeOperatorX::required_data_distribution (probe side):
            //   enable_broadcast_join_force_passthrough ? PASSTHROUGH
            //     : (_child->is_serial_operator() ? PASSTHROUGH : NOOP)
            // We mirror the force-passthrough session variable to match BE.  NOTE: for a
            // *non-serial* probe this is currently a no-op — enforceRequire only inserts a
            // PASSTHROUGH local exchange to fan a serial (1-task) source out to N tasks; an
            // already-N-task source satisfies passthrough so no exchange is added (verified on
            // a 4-BE cluster: identical plan and results vs BE-native, no crash).  Keeping the
            // check matches BE's intent and is in place should the framework later force the
            // exchange; a true rebalance of a non-serial probe is a perf-only follow-up.
            // getConnectContext() can be null (unit-test mocks); treat as no force.
            boolean forcePassthrough = translatorContext.getConnectContext() != null
                    && translatorContext.getConnectContext().getSessionVariable()
                            .enableBroadcastJoinForcePassthrough;
            boolean probeChildSerial = children.get(0).isSerialOperatorOnBe(
                    translatorContext.getConnectContext());
            boolean buildChildSerial = children.get(1).isSerialOperatorOnBe(
                    translatorContext.getConnectContext());
            boolean probePassthrough = forcePassthrough || probeChildSerial;
            probeSideRequire = probePassthrough
                    ? LocalExchangeTypeRequire.requirePassthrough()
                    : LocalExchangeTypeRequire.noRequire();
            buildSideRequire = buildChildSerial
                    ? LocalExchangeTypeRequire.requirePassToOne()
                    : LocalExchangeTypeRequire.noRequire();
            // For serial or force-passthrough probe: output is PASSTHROUGH.
            // For a non-serial probe without the flag: propagate the probe's distribution.
            outputType = probePassthrough ? LocalExchangeType.PASSTHROUGH : null;
        } else if (isColocate() || isBucketShuffle()) {
            // Both probe and build sides require BUCKET_HASH_SHUFFLE: the bucket distribution
            // must be preserved on both inputs. A serial child on either side is handled the
            // same way (serial exchange returns NOOP → enforceRequire() inserts the LE).
            probeSideRequire = LocalExchangeTypeRequire.requireBucketHash();
            // For BUCKET_SHUFFLE with serial build child: use requireBucketHash() (not
            // requirePassToOne()). Unlike BROADCAST joins, BUCKET_SHUFFLE has no shared
            // hash table mechanism — PASS_TO_ONE routes all data to task 0 while tasks 1..N-1
            // build empty hash tables, losing rows. BUCKET_HASH_SHUFFLE correctly distributes
            // build data by bucket to match the probe side's bucket distribution.
            // The serial exchange returns NOOP, so enforceRequire() will insert a
            // BUCKET_HASH_SHUFFLE local exchange (with PASSTHROUGH fan-out for heavy-ops
            // bottleneck avoidance).
            buildSideRequire = LocalExchangeTypeRequire.requireBucketHash();
            outputType = AddLocalExchange.resolveExchangeType(
                    LocalExchangeTypeRequire.requireBucketHash());
        } else {
            // PARTITIONED (shuffle) join: both sides enter via global hash exchange.
            // Require GLOBAL specifically so that any inserted exchange uses the same
            // instance mapping as the cross-fragment exchange. LOCAL hash has a different
            // modulus (per-BE instance count vs total instance count) and would cause
            // join mismatches (DORIS-26101).
            //
            // Exception: serial source (use_serial_exchange=true + pooling). The serial
            // exchange sends to a single BE so shuffle_idx_to_instance_idx has only one
            // entry — GLOBAL hash would route data to non-existent indices (DORIS-26120).
            // Fall back to generic requireHash() which resolves to LOCAL, matching BE's
            // _use_serial_source behavior.
            boolean serialSource = fragment != null
                    && fragment.useSerialSource(translatorContext.getConnectContext());
            buildSideRequire = probeSideRequire = serialSource
                    ? LocalExchangeTypeRequire.requireHash()
                    : LocalExchangeTypeRequire.requireGlobalExecutionHash();
            outputType = null; // derived from probeResult.second below
        }

        Pair<PlanNode, LocalExchangeType> probeResult = enforceRequire(
                translatorContext, children.get(0), 0, probeSideRequire);
        Pair<PlanNode, LocalExchangeType> buildResult = enforceRequire(
                translatorContext, children.get(1), 1, buildSideRequire);
        this.children = Lists.newArrayList(probeResult.first, buildResult.first);
        if (outputType == null) {
            outputType = probeResult.second;
        }
        return Pair.of(this, outputType);
    }

    @Override
    protected boolean shouldResetSerialFlagForChild(int childIndex) {
        return childIndex == 1;
    }
}
