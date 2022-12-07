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
import org.apache.doris.analysis.BitmapFilterPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.VectorizedUtil;
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

    private boolean isOutputLeftSideOnly = false;

    private List<Expr> runtimeFilterExpr = Lists.newArrayList();
    private List<Expr> joinConjuncts;

    private Expr vJoinConjunct;

    public NestedLoopJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef) {
        super(id, "NESTED LOOP JOIN", StatisticalType.NESTED_LOOP_JOIN_NODE, outer, inner, innerRef);
        tupleIds.addAll(outer.getTupleIds());
        tupleIds.addAll(inner.getTupleIds());
    }

    public boolean canParallelize() {
        return joinOp == JoinOperator.CROSS_JOIN || joinOp == JoinOperator.INNER_JOIN
                || joinOp == JoinOperator.LEFT_OUTER_JOIN || joinOp == JoinOperator.LEFT_ANTI_JOIN
                || joinOp == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
    }

    public void setJoinConjuncts(List<Expr> joinConjuncts) {
        this.joinConjuncts = joinConjuncts;
    }

    @Override
    protected List<SlotId> computeSlotIdsForJoinConjuncts(Analyzer analyzer) {
        // conjunct
        List<SlotId> conjunctSlotIds = Lists.newArrayList();
        Expr.getIds(joinConjuncts, null, conjunctSlotIds);
        return conjunctSlotIds;
    }

    @Override
    protected Pair<Boolean, Boolean> needToCopyRightAndLeft() {
        boolean copyleft = true;
        boolean copyRight = true;
        return Pair.of(copyleft, copyRight);
    }

    /**
     * Only for Nereids.
     */
    public NestedLoopJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, List<TupleId> tupleIds,
            JoinOperator joinOperator, List<Expr> srcToOutputList, TupleDescriptor intermediateTuple,
            TupleDescriptor outputTuple) {
        super(id, "NESTED LOOP JOIN", StatisticalType.NESTED_LOOP_JOIN_NODE, joinOperator);
        this.tupleIds.addAll(tupleIds);
        children.add(outer);
        children.add(inner);
        // TODO: need to set joinOp by Nereids

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        nullableTupleIds.addAll(inner.getNullableTupleIds());
        if (joinOp.equals(JoinOperator.FULL_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.LEFT_OUTER_JOIN)) {
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
        }
        vIntermediateTupleDescList = Lists.newArrayList(intermediateTuple);
        vOutputTupleDesc = outputTuple;
        vSrcToOutputSMap = new ExprSubstitutionMap(srcToOutputList, Collections.emptyList());
    }

    public void setOutputLeftSideOnly(boolean outputLeftSideOnly) {
        isOutputLeftSideOnly = outputLeftSideOnly;
    }

    public List<Expr> getRuntimeFilterExpr() {
        return runtimeFilterExpr;
    }

    public void addBitmapFilterExpr(Expr runtimeFilterExpr) {
        this.runtimeFilterExpr.add(runtimeFilterExpr);
    }

    public TableRef getInnerRef() {
        return innerRef;
    }

    @Override
    protected void computeOldCardinality() {
        if (getChild(0).cardinality == -1 || getChild(1).cardinality == -1) {
            cardinality = -1;
        } else {
            cardinality = getChild(0).cardinality * getChild(1).cardinality;
            if (computeOldSelectivity() != -1) {
                cardinality = Math.round(((double) cardinality) * computeOldSelectivity());
            }
        }
        LOG.debug("stats NestedLoopJoin: cardinality={}", Long.toString(cardinality));
    }

    @Override
    protected void computeOtherConjuncts(Analyzer analyzer, ExprSubstitutionMap originToIntermediateSmap) {
        joinConjuncts = Expr.substituteList(joinConjuncts, originToIntermediateSmap, analyzer, false);
        if (vJoinConjunct != null) {
            vJoinConjunct =
                    Expr.substituteList(Collections.singletonList(vJoinConjunct), originToIntermediateSmap, analyzer,
                                    false).get(0);
        }
    }

    @Override
    public void convertToVectoriezd() {
        if (!joinConjuncts.isEmpty()) {
            vJoinConjunct = convertConjunctsToAndCompoundPredicate(joinConjuncts);
            initCompoundPredicate(vJoinConjunct);
        }
        super.convertToVectoriezd();
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).addValue(super.debugString()).toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.nested_loop_join_node = new TNestedLoopJoinNode();
        msg.nested_loop_join_node.join_op = joinOp.toThrift();
        if (vJoinConjunct != null) {
            msg.nested_loop_join_node.setVjoinConjunct(vJoinConjunct.treeToThrift());
        }
        if (vSrcToOutputSMap != null) {
            for (int i = 0; i < vSrcToOutputSMap.size(); i++) {
                // TODO: Enable it after we support new optimizers
                // if (ConnectContext.get().getSessionVariable().isEnableNereidsPlanner()) {
                //     msg.addToProjections(vSrcToOutputSMap.getLhs().get(i).treeToThrift());
                // } else
                msg.nested_loop_join_node.addToSrcExprList(vSrcToOutputSMap.getLhs().get(i).treeToThrift());
            }
        }
        if (vOutputTupleDesc != null) {
            msg.nested_loop_join_node.setVoutputTupleId(vOutputTupleDesc.getId().asInt());
            // TODO Enable it after we support new optimizers
            // msg.setOutputTupleId(vOutputTupleDesc.getId().asInt());
        }
        if (vIntermediateTupleDescList != null) {
            for (TupleDescriptor tupleDescriptor : vIntermediateTupleDescList) {
                msg.nested_loop_join_node.addToVintermediateTupleIdList(tupleDescriptor.getId().asInt());
            }
        }
        msg.nested_loop_join_node.setIsOutputLeftSideOnly(isOutputLeftSideOnly);
        msg.node_type = TPlanNodeType.CROSS_JOIN_NODE;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        ExprSubstitutionMap combinedChildSmap = getCombinedChildWithoutTupleIsNullSmap();
        joinConjuncts = Expr.substituteList(joinConjuncts, combinedChildSmap, analyzer, false);
        computeCrossRuntimeFilterExpr();

        // Only for Vec: create new tuple for join result
        if (VectorizedUtil.isVectorized()) {
            computeOutputTuple(analyzer);
        }
    }

    private void computeCrossRuntimeFilterExpr() {
        for (int i = conjuncts.size() - 1; i >= 0; --i) {
            if (conjuncts.get(i) instanceof BitmapFilterPredicate) {
                addBitmapFilterExpr(conjuncts.get(i));
                conjuncts.remove(i);
            }
        }
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

        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("predicates: ").append(getExplainString(conjuncts)).append("\n");
        }
        if (!runtimeFilters.isEmpty()) {
            output.append(detailPrefix).append("runtime filters: ");
            output.append(getRuntimeFilterExplainString(true));
            output.append("isOutputLeftSideOnly: ").append(isOutputLeftSideOnly).append("\n");
        }
        output.append(detailPrefix).append(String.format("cardinality=%,d", cardinality)).append("\n");
        // todo unify in plan node
        if (vOutputTupleDesc != null) {
            output.append(detailPrefix).append("vec output tuple id: ").append(vOutputTupleDesc.getId()).append("\n");
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
        return output.toString();
    }
}
