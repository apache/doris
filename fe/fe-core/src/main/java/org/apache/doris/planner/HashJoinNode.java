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
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.nereids.trees.expressions.ExprId;
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

    public HashJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, JoinOperator joinOp,
            List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts,
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
        this.markJoinConjuncts = markJoinConjuncts;
        children.add(outer);
        children.add(inner);

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(inner.getNullableTupleIds());
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        if (joinOp.equals(JoinOperator.FULL_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getOutputTupleIds());
            nullableTupleIds.addAll(inner.getOutputTupleIds());
        } else if (joinOp.equals(JoinOperator.LEFT_OUTER_JOIN)) {
            nullableTupleIds.addAll(inner.getOutputTupleIds());
        } else if (joinOp.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getOutputTupleIds());
        }
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
            TEqJoinCondition eqJoinCondition = new TEqJoinCondition(eqJoinPredicate.getChild(0).treeToThrift(),
                    eqJoinPredicate.getChild(1).treeToThrift());
            eqJoinCondition.setOpcode(eqJoinPredicate.getOp().getOpcode());
            msg.hash_join_node.addToEqJoinConjuncts(eqJoinCondition);
        }
        for (Expr e : otherJoinConjuncts) {
            msg.hash_join_node.addToOtherJoinConjuncts(e.treeToThrift());
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
                            e.getChild(0).treeToThrift(), e.getChild(1).treeToThrift());
                    eqJoinCondition.setOpcode(((BinaryPredicate) e).getOp().getOpcode());
                    msg.hash_join_node.addToEqJoinConjuncts(eqJoinCondition);
                }
            } else {
                for (Expr e : markJoinConjuncts) {
                    msg.hash_join_node.addToMarkJoinConjuncts(e.treeToThrift());
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
            output.append(detailPrefix).append("equal join conjunct: ").append(eqJoinPredicate.toSql()).append("\n");
        }
        if (!otherJoinConjuncts.isEmpty()) {
            output.append(detailPrefix).append("other join predicates: ")
                    .append(getExplainString(otherJoinConjuncts)).append("\n");
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

    public enum DistributionMode {
        NONE("NONE"),
        BROADCAST("BROADCAST"),
        PARTITIONED("PARTITIONED"),
        BUCKET_SHUFFLE("BUCKET_SHUFFLE");

        private final String description;

        DistributionMode(String descr) {
            this.description = descr;
        }

        @Override
        public String toString() {
            return description;
        }

        public TJoinDistributionType toThrift() {
            switch (this) {
                case NONE:
                    return TJoinDistributionType.NONE;
                case BROADCAST:
                    return TJoinDistributionType.BROADCAST;
                case PARTITIONED:
                    return TJoinDistributionType.PARTITIONED;
                case BUCKET_SHUFFLE:
                    return TJoinDistributionType.BUCKET_SHUFFLE;
                default:
                    Preconditions.checkArgument(false, "Unknown DistributionMode: " + this);
            }
            return TJoinDistributionType.NONE;
        }
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
}
