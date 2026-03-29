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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TGroupJoinNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Group join node: fuses a hash join with an aggregation.
 * During the probe phase, matched rows are directly aggregated
 * instead of being materialized as intermediate results.
 */
public class GroupJoinNode extends HashJoinNode {

    private final AggregateInfo aggInfo;
    private boolean needsFinalize;
    private boolean aggIsColocate = false;
    private TupleDescriptor joinOutputTupleDesc;

    public GroupJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, JoinOperator joinOp,
            List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts,
            AggregateInfo aggInfo) {
        super(id, outer, inner, joinOp, eqJoinConjuncts, otherJoinConjuncts,
                null /* matchCondition */, Lists.newArrayList() /* markJoinConjuncts */, false /* isMarkJoin */);
        this.aggInfo = aggInfo;
        this.needsFinalize = true;
        setPlanNodeName("GROUP JOIN");
    }

    public void unsetNeedsFinalize() {
        needsFinalize = false;
    }

    public void setAggColocate(boolean colocate) {
        aggIsColocate = colocate;
    }

    public void setJoinOutputTupleDesc(TupleDescriptor tupleDesc) {
        this.joinOutputTupleDesc = tupleDesc;
    }

    public AggregateInfo getAggInfo() {
        return aggInfo;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.GROUP_JOIN_NODE;
        TGroupJoinNode groupJoinNode = new TGroupJoinNode();

        // --- Hash join fields ---
        groupJoinNode.setJoinOp(getJoinOp().toThrift());
        for (Expr eqJoinPredicate : getEqJoinConjuncts()) {
            org.apache.doris.thrift.TEqJoinCondition eqJoinCondition =
                    new org.apache.doris.thrift.TEqJoinCondition(
                            ExprToThriftVisitor.treeToThrift(eqJoinPredicate.getChild(0)),
                            ExprToThriftVisitor.treeToThrift(eqJoinPredicate.getChild(1)));
            eqJoinCondition.setOpcode(
                    ExprToThriftVisitor.toThriftOpcode(
                            ((org.apache.doris.analysis.BinaryPredicate) eqJoinPredicate).getOp()));
            groupJoinNode.addToEqJoinConjuncts(eqJoinCondition);
        }
        List<Expr> otherConjuncts = getOtherJoinConjuncts();
        if (otherConjuncts != null && !otherConjuncts.isEmpty()) {
            groupJoinNode.setVotherJoinConjunct(ExprToThriftVisitor.treeToThrift(otherConjuncts.get(0)));
        }
        if (getHashOutputSlotIds() != null) {
            for (org.apache.doris.analysis.SlotId slotId : getHashOutputSlotIds()) {
                groupJoinNode.addToHashOutputSlotIds(slotId.asInt());
            }
        }
        if (vIntermediateTupleDescList != null) {
            for (org.apache.doris.analysis.TupleDescriptor td : vIntermediateTupleDescList) {
                groupJoinNode.addToVintermediateTupleIdList(td.getId().asInt());
            }
        }
        groupJoinNode.setIsBroadcastJoin(getDistributionMode() == DistributionMode.BROADCAST);
        groupJoinNode.setDistType(getDistributionMode().toThrift());
        if (joinOutputTupleDesc != null) {
            groupJoinNode.setJoinOutputTupleId(joinOutputTupleDesc.getId().asInt());
        }

        // --- Aggregation fields ---
        aggInfo.updateMaterializedSlots();
        List<TExpr> aggregateFunctions = Lists.newArrayList();
        for (FunctionCallExpr e : aggInfo.getMaterializedAggregateExprs()) {
            aggregateFunctions.add(ExprToThriftVisitor.treeToThrift(e));
        }
        groupJoinNode.setAggregateFunctions(aggregateFunctions);
        groupJoinNode.setAggIntermediateTupleId(aggInfo.getOutputTupleId().asInt());
        groupJoinNode.setAggOutputTupleId(aggInfo.getOutputTupleId().asInt());
        groupJoinNode.setNeedFinalize(needsFinalize);
        groupJoinNode.setIsFirstPhase(aggInfo.isFirstPhase());
        groupJoinNode.setIsColocate(aggIsColocate);
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        if (groupingExprs != null) {
            groupJoinNode.setGroupingExprs(ExprToThriftVisitor.treesToThrift(groupingExprs));
        }

        msg.group_join_node = groupJoinNode;
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(super.getNodeExplainString(detailPrefix, detailLevel));
        output.append(detailPrefix).append("group by: ")
                .append(getExplainString(aggInfo.getGroupingExprs())).append("\n");
        if (aggInfo.getMaterializedAggregateExprs().size() > 0) {
            output.append(detailPrefix).append("agg functions: ")
                    .append(getExplainString(aggInfo.getMaterializedAggregateExprs())).append("\n");
        }
        return output.toString();
    }
}
