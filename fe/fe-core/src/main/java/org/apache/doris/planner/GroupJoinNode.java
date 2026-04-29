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
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.thrift.TEqJoinCondition;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TGroupJoinNode;
import org.apache.doris.thrift.THashJoinNode;
import org.apache.doris.thrift.TJoinDistributionType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Legacy planner node for GroupJoin.
 */
public class GroupJoinNode extends JoinNodeBase {

    private final List<Expr> eqJoinConjuncts;
    private final List<Expr> groupByExprs;
    private final List<FunctionCallExpr> aggregateFunctions;
    private final Expr otherJoinConjunct;

    public GroupJoinNode(PlanNodeId id, PlanNode probeChild, PlanNode buildChild,
                         JoinOperator joinOp, List<Expr> eqJoinConjuncts, Expr otherJoinConjunct,
                         List<Expr> groupByExprs, List<FunctionCallExpr> aggregateFunctions) {
        super(id, "GROUP JOIN", joinOp, false);
        this.eqJoinConjuncts = eqJoinConjuncts;
        this.otherJoinConjunct = otherJoinConjunct;
        this.groupByExprs = groupByExprs;
        this.aggregateFunctions = aggregateFunctions;
        this.children.add(probeChild);
        this.children.add(buildChild);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.GROUP_JOIN_NODE;

        msg.group_join_node = new TGroupJoinNode();
        msg.group_join_node.join_op = joinOp.toThrift();
        List<TEqJoinCondition> eqConditions = new ArrayList<>();
        for (Expr conjunct : eqJoinConjuncts) {
            BinaryPredicate eqJoinPredicate = (BinaryPredicate) conjunct;
            TEqJoinCondition eqJoinCondition = new TEqJoinCondition(
                    ExprToThriftVisitor.treeToThrift(eqJoinPredicate.getChild(0)),
                    ExprToThriftVisitor.treeToThrift(eqJoinPredicate.getChild(1)));
            eqJoinCondition.setOpcode(ExprToThriftVisitor.toThriftOpcode(eqJoinPredicate.getOp()));
            eqConditions.add(eqJoinCondition);
        }
        msg.group_join_node.eq_join_conjuncts = eqConditions;
        if (otherJoinConjunct != null) {
            msg.group_join_node.vother_join_conjunct = ExprToThriftVisitor.treeToThrift(otherJoinConjunct);
        }
        msg.group_join_node.grouping_exprs = ExprToThriftVisitor.treesToThrift(groupByExprs);
        msg.group_join_node.aggregate_functions = ExprToThriftVisitor.treesToThrift(aggregateFunctions);

        // Also populate hash_join_node so that BE HashJoin operators can fall back
        // to HashJoin execution in PR1.
        msg.hash_join_node = new THashJoinNode();
        msg.hash_join_node.join_op = joinOp.toThrift();
        msg.hash_join_node.eq_join_conjuncts = eqConditions;
        if (otherJoinConjunct != null) {
            msg.hash_join_node.vother_join_conjunct = ExprToThriftVisitor.treeToThrift(otherJoinConjunct);
        }
        msg.hash_join_node.is_broadcast_join = false;
        msg.hash_join_node.dist_type = TJoinDistributionType.PARTITIONED;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("GROUP JOIN [").append(joinOp).append("]\n");
        output.append(prefix).append("  GROUP BY: ").append(groupByExprs).append("\n");
        output.append(prefix).append("  AGGREGATE: ").append(aggregateFunctions).append("\n");
        output.append(prefix).append("  EQ JOIN CONJUNCTS: ").append(eqJoinConjuncts).append("\n");
        if (otherJoinConjunct != null) {
            output.append(prefix).append("  OTHER JOIN CONJUNCTS: ").append(otherJoinConjunct).append("\n");
        }
        return output.toString();
    }
}
