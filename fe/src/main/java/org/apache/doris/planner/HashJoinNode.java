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
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.ColumnStats;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TEqJoinCondition;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.THashJoinNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Hash join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 */
public class HashJoinNode extends PlanNode {
    private final static Logger LOG = LogManager.getLogger(HashJoinNode.class);

    private final TableRef     innerRef;
    private final JoinOperator joinOp;
    // predicates of the form 'a=b' or 'a<=>b'
    private List<BinaryPredicate> eqJoinConjuncts = Lists.newArrayList();
    // join conjuncts from the JOIN clause that aren't equi-join predicates
    private  List<Expr> otherJoinConjuncts;
    private boolean isPushDown;
    private DistributionMode distrMode;
    private boolean isColocate = false; //the flag for colocate join
    private String colocateReason = ""; // if can not do colocate join, set reason here

    public HashJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef,
                        List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
        super(id, "HASH JOIN");
        Preconditions.checkArgument(eqJoinConjuncts != null && !eqJoinConjuncts.isEmpty());
        Preconditions.checkArgument(otherJoinConjuncts != null);
        tupleIds.addAll(outer.getTupleIds());
        tupleIds.addAll(inner.getTupleIds());
        tblRefIds.addAll(outer.getTblRefIds());
        tblRefIds.addAll(inner.getTblRefIds());
        this.innerRef = innerRef;
        this.joinOp = innerRef.getJoinOp();
        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            Preconditions.checkArgument(eqJoinPredicate instanceof BinaryPredicate);
            this.eqJoinConjuncts.add((BinaryPredicate) eqJoinPredicate);
        }
        this.distrMode = DistributionMode.NONE;
        this.otherJoinConjuncts = otherJoinConjuncts;
        children.add(outer);
        children.add(inner);
        this.isPushDown = false;

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(inner.getNullableTupleIds());
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        if (joinOp.equals(JoinOperator.FULL_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.LEFT_OUTER_JOIN)) {
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
        }
    }

    public List<BinaryPredicate> getEqJoinConjuncts() {
        return eqJoinConjuncts;
    }

    public JoinOperator getJoinOp() {
        return joinOp;
    }

    public TableRef getInnerRef() {
        return innerRef;
    }

    public DistributionMode getDistributionMode() {
        return distrMode;
    }

    public void setDistributionMode(DistributionMode distrMode) {
        this.distrMode = distrMode;
    }

    public boolean isColocate() {
        return isColocate;
    }

    public void setColocate(boolean colocate, String reason) {
        isColocate = colocate;
        colocateReason = reason;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        assignConjuncts(analyzer);

        // Set smap to the combined children's smaps and apply that to all conjuncts_.
        createDefaultSmap(analyzer);

        computeStats(analyzer);
        //assignedConjuncts = analyzr.getAssignedConjuncts();

        ExprSubstitutionMap combinedChildSmap = getCombinedChildWithoutTupleIsNullSmap();
        List<Expr> newEqJoinConjuncts =
                Expr.substituteList(eqJoinConjuncts, combinedChildSmap, analyzer, false);
        eqJoinConjuncts = newEqJoinConjuncts.stream()
                .map(entity -> (BinaryPredicate) entity).collect(Collectors.toList());
        otherJoinConjuncts =
                Expr.substituteList(otherJoinConjuncts, combinedChildSmap, analyzer, false);
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);

        // For a join between child(0) and child(1), we look for join conditions "L.c = R.d"
        // (with L being from child(0) and R from child(1)) and use as the cardinality
        // estimate the maximum of
        //   child(0).cardinality * R.cardinality / # distinct values for R.d
        //     * child(1).cardinality / R.cardinality
        // across all suitable join conditions, which simplifies to
        //   child(0).cardinality * child(1).cardinality / # distinct values for R.d
        // The reasoning is that
        // - each row in child(0) joins with R.cardinality/#DV_R.d rows in R
        // - each row in R is 'present' in child(1).cardinality / R.cardinality rows in
        //   child(1)
        //
        // This handles the very frequent case of a fact table/dimension table join
        // (aka foreign key/primary key join) if the primary key is a single column, with
        // possible additional predicates against the dimension table. An example:
        // FROM FactTbl F JOIN Customers C D ON (F.cust_id = C.id) ... WHERE C.region = 'US'
        // - if there are 5 regions, the selectivity of "C.region = 'US'" would be 0.2
        //   and the output cardinality of the Customers scan would be 0.2 * # rows in
        //   Customers
        // - # rows in Customers == # of distinct values for Customers.id
        // - the output cardinality of the join would be F.cardinality * 0.2

        long maxNumDistinct = 0;
        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            Expr lhsJoinExpr = eqJoinPredicate.getChild(0);
            Expr rhsJoinExpr = eqJoinPredicate.getChild(1);
            if (lhsJoinExpr.unwrapSlotRef() == null) {
                continue;
            }
            SlotRef rhsSlotRef = rhsJoinExpr.unwrapSlotRef();
            if (rhsSlotRef == null) {
                continue;
            }
            SlotDescriptor slotDesc = rhsSlotRef.getDesc();
            if (slotDesc == null) {
                continue;
            }
            ColumnStats stats = slotDesc.getStats();
            if (!stats.hasNumDistinctValues()) {
                continue;
            }
            long numDistinct = stats.getNumDistinctValues();
            // TODO rownum
            //Table rhsTbl = slotDesc.getParent().getTableFamilyGroup().getBaseTable();
            // if (rhsTbl != null && rhsTbl.getNumRows() != -1) {
                // we can't have more distinct values than rows in the table, even though
                // the metastore stats may think so
                // LOG.info(
                //   "#distinct=" + numDistinct + " #rows=" + Long.toString(rhsTbl.getNumRows()));
                // numDistinct = Math.min(numDistinct, rhsTbl.getNumRows());
            // }
            maxNumDistinct = Math.max(maxNumDistinct, numDistinct);
            LOG.debug("min slotref: {}, #distinct: {}", rhsSlotRef.toSql(), numDistinct);
        }

        if (maxNumDistinct == 0) {
            // if we didn't find any suitable join predicates or don't have stats
            // on the relevant columns, we very optimistically assume we're doing an
            // FK/PK join (which doesn't alter the cardinality of the left-hand side)
            cardinality = getChild(0).cardinality;
        } else {
            cardinality = Math.round((double) getChild(0).cardinality * (double) getChild(
                1).cardinality / (double) maxNumDistinct);
            LOG.debug("lhs card: {}, rhs card: {}", getChild(0).cardinality, getChild(1).cardinality);
        }
        LOG.debug("stats HashJoin: cardinality {}", cardinality);
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("eqJoinConjuncts",
          eqJoinConjunctsDebugString()).addValue(super.debugString()).toString();
    }

    private String eqJoinConjunctsDebugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        for (BinaryPredicate expr : eqJoinConjuncts) {
            helper.add("lhs", expr.getChild(0)).add("rhs", expr.getChild(1));
        }
        return helper.toString();
    }

    @Override
    public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
        super.getMaterializedIds(analyzer, ids);
        // we also need to materialize everything referenced by eqJoinConjuncts
        // and otherJoinConjuncts
        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            eqJoinPredicate.getIds(null, ids);
        }
        for (Expr e : otherJoinConjuncts) {
            e.getIds(null, ids);
        }
    }

    public void setIsPushDown(boolean isPushDown) {
        this.isPushDown = isPushDown;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
        msg.hash_join_node = new THashJoinNode();
        msg.hash_join_node.join_op = joinOp.toThrift();
        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            TEqJoinCondition eqJoinCondition = new TEqJoinCondition(eqJoinPredicate.getChild(0).treeToThrift(),
                    eqJoinPredicate.getChild(1).treeToThrift());
            eqJoinCondition.setOpcode(eqJoinPredicate.getOp().getOpcode());
            msg.hash_join_node.addToEq_join_conjuncts(eqJoinCondition);
        }
        for (Expr e : otherJoinConjuncts) {
            msg.hash_join_node.addToOther_join_conjuncts(e.treeToThrift());
        }
        msg.hash_join_node.setIs_push_down(isPushDown);
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        String distrModeStr =
          (distrMode != DistributionMode.NONE) ? (" (" + distrMode.toString() + ")") : "";
        StringBuilder output = new StringBuilder().append(
          detailPrefix + "join op: " + joinOp.toString() + distrModeStr + "\n").append(
          detailPrefix + "hash predicates:\n");

        output.append(detailPrefix + "colocate: " + isColocate + (isColocate? "" : ", reason: " + colocateReason) + "\n");

        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            output.append(detailPrefix).append("equal join conjunct: ").append(eqJoinPredicate.toSql() +  "\n");
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

    public boolean isShuffleJoin() {
        return distrMode == DistributionMode.PARTITIONED;
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
