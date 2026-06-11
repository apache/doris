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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNestedLoopJoinNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

/**
 * Nested loop join between left child and right child.
 */
public class NestedLoopJoinNode extends JoinNodeBase {
    private static final Logger LOG = LogManager.getLogger(NestedLoopJoinNode.class);

    private List<Expr> runtimeFilterExpr = Lists.newArrayList();
    private List<Expr> joinConjuncts;

    private List<Expr> markJoinConjuncts;

    public static boolean canParallelize(JoinOperator joinOp) {
        return joinOp == JoinOperator.CROSS_JOIN || joinOp == JoinOperator.INNER_JOIN
                || joinOp == JoinOperator.LEFT_OUTER_JOIN || joinOp == JoinOperator.LEFT_SEMI_JOIN
                || joinOp == JoinOperator.LEFT_ANTI_JOIN || joinOp == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN
                || joinOp == JoinOperator.ASOF_LEFT_INNER_JOIN || joinOp == JoinOperator.ASOF_RIGHT_INNER_JOIN
                || joinOp == JoinOperator.ASOF_LEFT_OUTER_JOIN;
    }


    public void setJoinConjuncts(List<Expr> joinConjuncts) {
        this.joinConjuncts = joinConjuncts;
    }

    public void setMarkJoinConjuncts(List<Expr> markJoinConjuncts) {
        this.markJoinConjuncts = markJoinConjuncts;
    }


    /**
     * Only for Nereids.
     */
    public NestedLoopJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, List<TupleId> tupleIds,
            JoinOperator joinOperator, List<Expr> srcToOutputList, TupleDescriptor intermediateTuple,
            TupleDescriptor outputTuple, boolean isMarkJoin) {
        super(id, "NESTED LOOP JOIN", StatisticalType.NESTED_LOOP_JOIN_NODE, joinOperator, isMarkJoin);
        this.tupleIds.addAll(tupleIds);
        children.add(outer);
        children.add(inner);
        // TODO: need to set joinOp by Nereids

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        nullableTupleIds.addAll(inner.getNullableTupleIds());
        if (joinOp.equals(JoinOperator.FULL_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getOutputTupleIds());
            nullableTupleIds.addAll(inner.getOutputTupleIds());
        } else if (joinOp.equals(JoinOperator.LEFT_OUTER_JOIN)) {
            nullableTupleIds.addAll(inner.getOutputTupleIds());
        } else if (joinOp.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getOutputTupleIds());
        }
        vIntermediateTupleDescList = Lists.newArrayList(intermediateTuple);
        outputTupleDesc = outputTuple;
        vSrcToOutputSMap = new ExprSubstitutionMap(srcToOutputList, Collections.emptyList());
    }

    public List<Expr> getRuntimeFilterExpr() {
        return runtimeFilterExpr;
    }


    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).addValue(super.debugString()).toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.nested_loop_join_node = new TNestedLoopJoinNode();
        msg.nested_loop_join_node.join_op = joinOp.toThrift();
        for (Expr conjunct : joinConjuncts) {
            msg.nested_loop_join_node.addToJoinConjuncts(conjunct.treeToThrift());
        }
        if (markJoinConjuncts != null) {
            for (Expr conjunct : markJoinConjuncts) {
                msg.nested_loop_join_node.addToMarkJoinConjuncts(conjunct.treeToThrift());
            }
        }

        msg.nested_loop_join_node.setIsMark(isMarkJoin());

        if (vIntermediateTupleDescList != null) {
            for (TupleDescriptor tupleDescriptor : vIntermediateTupleDescList) {
                msg.nested_loop_join_node.addToVintermediateTupleIdList(tupleDescriptor.getId().asInt());
            }
        }
        msg.nested_loop_join_node.setUseSpecificProjections(false);
        msg.node_type = TPlanNodeType.CROSS_JOIN_NODE;
    }


    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        String distrModeStr = "";
        StringBuilder output =
                new StringBuilder().append(detailPrefix).append("join op: ").append(joinOp.toString()).append("(")
                        .append(distrModeStr).append(")\n");

        if (detailLevel == TExplainLevel.BRIEF) {
            output.append(detailPrefix).append(
                    String.format("cardinality=%,d", cardinality)).append("\n");
            return output.toString();
        }

        if (!joinConjuncts.isEmpty()) {
            output.append(detailPrefix).append("join conjuncts: ").append(getExplainString(joinConjuncts)).append("\n");
        }

        if (markJoinConjuncts != null && !markJoinConjuncts.isEmpty()) {
            output.append(detailPrefix).append("mark join predicates: ")
                    .append(getExplainString(markJoinConjuncts)).append("\n");
        }

        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("predicates: ").append(getExplainString(conjuncts)).append("\n");
        }

        output.append(detailPrefix).append(String.format("cardinality=%,d", cardinality)).append("\n");

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
        if (detailLevel == TExplainLevel.VERBOSE) {
            output.append(detailPrefix).append("isMarkJoin: ").append(isMarkJoin()).append("\n");
        }
        return output.toString();
    }

    /**
     * If joinOp is one of type below:
     * 1. RIGHT_OUTER_JOIN
     * 2. RIGHT_ANTI_JOIN
     * 3. RIGHT_SEMI_JOIN
     * 4. FULL_OUTER_JOIN
     *
     * Probe-side must have full data so join is a serial operator.
     */
    @Override
    public boolean isSerialOperator() {
        return joinOp == JoinOperator.RIGHT_OUTER_JOIN || joinOp == JoinOperator.RIGHT_ANTI_JOIN
                || joinOp == JoinOperator.RIGHT_SEMI_JOIN || joinOp == JoinOperator.FULL_OUTER_JOIN;
    }
}
