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
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNestedLoopJoinNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Nested loop join between left child and right child.
 */
public class NestedLoopJoinNode extends JoinNodeBase {
    private List<Expr> joinConjuncts;

    private List<Expr> markJoinConjuncts;

    private final Set<SlotId> materializedSlotIds = Sets.newHashSet();

    private final Map<ExprId, SlotId> materializedSlotIdMap = Maps.newHashMap();

    private boolean hasMaterializedSlotIds = false;

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

    public Set<SlotId> getMaterializedSlotIds() {
        return materializedSlotIds;
    }

    public Map<ExprId, SlotId> getMaterializedSlotIdMap() {
        return materializedSlotIdMap;
    }

    public void enableMaterializedSlotIds() {
        hasMaterializedSlotIds = true;
    }

    public void addSlotIdToMaterializedSlotIds(SlotId slotId) {
        materializedSlotIds.add(slotId);
        hasMaterializedSlotIds = true;
    }

    private List<SlotId> getSortedMaterializedSlotIds() {
        List<SlotId> sortedSlotIds = new ArrayList<>(materializedSlotIds);
        sortedSlotIds.sort(Comparator.comparingInt(SlotId::asInt));
        return sortedSlotIds;
    }

    public NestedLoopJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, List<TupleId> tupleIds,
            JoinOperator joinOperator, boolean isMarkJoin) {
        super(id, "NESTED LOOP JOIN", joinOperator, isMarkJoin);
        this.tupleIds.addAll(tupleIds);
        children.add(outer);
        children.add(inner);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.nested_loop_join_node = new TNestedLoopJoinNode();
        msg.nested_loop_join_node.join_op = joinOp.toThrift();
        for (Expr conjunct : joinConjuncts) {
            msg.nested_loop_join_node.addToJoinConjuncts(ExprToThriftVisitor.treeToThrift(conjunct));
        }
        if (markJoinConjuncts != null) {
            for (Expr conjunct : markJoinConjuncts) {
                msg.nested_loop_join_node.addToMarkJoinConjuncts(ExprToThriftVisitor.treeToThrift(conjunct));
            }
        }

        msg.nested_loop_join_node.setIsMark(isMarkJoin());

        if (vIntermediateTupleDescList != null) {
            for (TupleDescriptor tupleDescriptor : vIntermediateTupleDescList) {
                msg.nested_loop_join_node.addToVintermediateTupleIdList(tupleDescriptor.getId().asInt());
            }
        }
        msg.nested_loop_join_node.setUseSpecificProjections(false);
        if (hasMaterializedSlotIds) {
            List<Integer> slotIds = new ArrayList<>();
            for (SlotId slotId : getSortedMaterializedSlotIds()) {
                slotIds.add(slotId.asInt());
            }
            msg.nested_loop_join_node.setMaterializedSlotIds(slotIds);
        }
        msg.node_type = TPlanNodeType.CROSS_JOIN_NODE;
    }


    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output =
                new StringBuilder().append(detailPrefix).append("join op: ").append(joinOp.toString()).append("()\n");

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
        if (hasMaterializedSlotIds) {
            output.append(detailPrefix).append("materialized slot ids: ");
            for (SlotId slotId : getSortedMaterializedSlotIds()) {
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
    public boolean isSerialNode() {
        return joinOp == JoinOperator.RIGHT_OUTER_JOIN || joinOp == JoinOperator.RIGHT_ANTI_JOIN
                || joinOp == JoinOperator.RIGHT_SEMI_JOIN || joinOp == JoinOperator.FULL_OUTER_JOIN;
    }

    @Override
    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(PlanTranslatorContext translatorContext,
            PlanNode parent, LocalExchangeTypeRequire parentRequire) {

        // Pooling mode: the fragment uses serial source (pooling scan or serial exchange).
        // NLJ build side needs BROADCAST in pooling mode so all probe tasks see full build data.
        boolean childUsePoolingScan = fragment.useSerialSource(translatorContext.getConnectContext());

        LocalExchangeTypeRequire probeSideRequire;
        LocalExchangeTypeRequire buildSideRequire;
        LocalExchangeType outputType;
        if (joinOp == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) {
            probeSideRequire = buildSideRequire = LocalExchangeTypeRequire.noRequire();
            outputType = LocalExchangeType.NOOP;
        } else if (isSerialNode()) {
            // RIGHT_OUTER/RIGHT_SEMI/RIGHT_ANTI/FULL_OUTER: probe side must be serial (1 task).
            // Build side: noRequire() — inserting BROADCAST would inflate build pipeline's
            // num_tasks while probe stays at 1, crashing in set_ready_to_read().
            probeSideRequire = LocalExchangeTypeRequire.noRequire();
            buildSideRequire = LocalExchangeTypeRequire.noRequire();
            outputType = LocalExchangeType.NOOP;
        } else if (childUsePoolingScan) {
            probeSideRequire = LocalExchangeTypeRequire.requireAdaptivePassthrough();
            buildSideRequire = LocalExchangeTypeRequire.requireBroadcast();
            outputType = LocalExchangeType.ADAPTIVE_PASSTHROUGH;
        } else {
            probeSideRequire = LocalExchangeTypeRequire.requireAdaptivePassthrough();
            buildSideRequire = LocalExchangeTypeRequire.noRequire();
            outputType = LocalExchangeType.ADAPTIVE_PASSTHROUGH;
        }

        // Both sides use enforceRequire — it handles serial flag propagation, satisfy
        // check (skip LE when child already outputs the required type, e.g., chained NLJs),
        // serial ancestor skip, and serial child fallback (auto-upgrade noRequire to
        // requirePassthrough when child is serial but this node is not).
        PlanNode probeSide = enforceRequire(
                translatorContext, children.get(0), 0, probeSideRequire).first;
        PlanNode buildSide = enforceRequire(
                translatorContext, children.get(1), 1, buildSideRequire).first;
        this.children = Lists.newArrayList(probeSide, buildSide);
        return Pair.of(this, outputType);
    }

    @Override
    protected boolean shouldResetSerialFlagForChild(int childIndex) {
        // Build side (child 1) is a separate pipeline in BE.  Normally,
        // the serial-ancestor flag should be reset across pipeline boundaries.
        // BUT when NLJ itself is serial (RIGHT_OUTER/ANTI/SEMI/FULL_OUTER),
        // the probe pipeline has num_tasks=1.  If we reset the flag, the
        // build-side Exchange may insert PASSTHROUGH (restoring num_tasks to
        // _num_instances), creating more build tasks than probe tasks.  The
        // extra build tasks have a NLJ shared state with empty source_deps,
        // crashing in set_ready_to_read().
        return childIndex == 1 && !isSerialNode();
    }
}
