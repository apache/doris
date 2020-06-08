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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TEqJoinCondition;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TMergeJoinNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Merge join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 */
public class MergeJoinNode extends PlanNode {
    private final static Logger LOG = LogManager.getLogger(MergeJoinNode.class);
    // conjuncts of the form "<lhs> = <rhs>", recorded as Pair(<lhs>, <rhs>)
    private final List<Pair<Expr, Expr>> cmpConjuncts;
    // join conjuncts from the JOIN clause that aren't equi-join predicates
    private final List<Expr> otherJoinConjuncts;
    private DistributionMode distrMode;

    public MergeJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner,
      List<Pair<Expr, Expr>> cmpConjuncts, List<Expr> otherJoinConjuncts) {
        super(id, "MERGE JOIN");
        Preconditions.checkArgument(cmpConjuncts != null);
        Preconditions.checkArgument(otherJoinConjuncts != null);
        tupleIds.addAll(outer.getTupleIds());
        tupleIds.addAll(inner.getTupleIds());
        this.distrMode = DistributionMode.PARTITIONED;
        this.cmpConjuncts = cmpConjuncts;
        this.otherJoinConjuncts = otherJoinConjuncts;
        children.add(outer);
        children.add(inner);

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(inner.getNullableTupleIds());
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        nullableTupleIds.addAll(outer.getTupleIds());
        nullableTupleIds.addAll(inner.getTupleIds());
    }

    public List<Pair<Expr, Expr>> getCmpConjuncts() {
        return cmpConjuncts;
    }

    public DistributionMode getDistributionMode() {
        return distrMode;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("cmpConjuncts", cmpConjunctsDebugString()).addValue(
          super.debugString()).toString();
    }

    private String cmpConjunctsDebugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        for (Pair<Expr, Expr> entry : cmpConjuncts) {
            helper.add("lhs", entry.first).add("rhs", entry.second);
        }
        return helper.toString();
    }

    @Override
    public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
        super.getMaterializedIds(analyzer, ids);
        // we also need to materialize everything referenced by cmpConjuncts
        // and otherJoinConjuncts
        for (Pair<Expr, Expr> p : cmpConjuncts) {
            p.first.getIds(null, ids);
            p.second.getIds(null, ids);
        }
        for (Expr e : otherJoinConjuncts) {
            e.getIds(null, ids);
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.MERGE_JOIN_NODE;
        msg.merge_join_node = new TMergeJoinNode();
        for (Pair<Expr, Expr> entry : cmpConjuncts) {
            TEqJoinCondition eqJoinCondition =
              new TEqJoinCondition(entry.first.treeToThrift(), entry.second.treeToThrift());
            msg.merge_join_node.addToCmp_conjuncts(eqJoinCondition);
        }
        for (Expr e : otherJoinConjuncts) {
            msg.hash_join_node.addToOther_join_conjuncts(e.treeToThrift());
        }
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        String distrModeStr =
          (distrMode != DistributionMode.NONE) ? (" (" + distrMode.toString() + ")") : "";
        StringBuilder output = new StringBuilder().append(
          detailPrefix + "join op: MERGE JOIN" + distrModeStr + "\n").append(
          detailPrefix + "hash predicates:\n");
        for (Pair<Expr, Expr> entry : cmpConjuncts) {
            output.append(detailPrefix + "  " +
              entry.first.toSql() + " = " + entry.second.toSql() + "\n");
        }
        if (!otherJoinConjuncts.isEmpty()) {
            output.append(detailPrefix + "other join predicates: ").append(
              getExplainString(otherJoinConjuncts) + "\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix + "other predicates: ").append(
              getExplainString(conjuncts) + "\n");
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return Math.max(children.get(0).getNumInstances(), children.get(1).getNumInstances());
    }

    enum DistributionMode {
        NONE("NONE"),
        BROADCAST("BROADCAST"),
        PARTITIONED("PARTITIONED");

        private final String description;

        private DistributionMode(String descr) {
            this.description = descr;
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
