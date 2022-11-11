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
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNestedLoopJoinNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Cross join between left child and right child.
 */
public class CrossJoinNode extends JoinNodeBase {
    private static final Logger LOG = LogManager.getLogger(CrossJoinNode.class);

    public CrossJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef) {
        super(id, "CROSS JOIN", StatisticalType.CROSS_JOIN_NODE, outer, inner, innerRef);
        tupleIds.addAll(outer.getTupleIds());
        tupleIds.addAll(inner.getTupleIds());
    }

    @Override
    public Set<SlotId> computeInputSlotIds(Analyzer analyzer) throws NotImplementedException {
        Set<SlotId> result = Sets.newHashSet();
        Preconditions.checkState(outputSlotIds != null);
        // step1: change output slot id to src slot id
        if (vSrcToOutputSMap != null) {
            for (SlotId slotId : outputSlotIds) {
                SlotRef slotRef = new SlotRef(analyzer.getDescTbl().getSlotDesc(slotId));
                Expr srcExpr = vSrcToOutputSMap.mappingForRhsExpr(slotRef);
                if (srcExpr == null) {
                    result.add(slotId);
                } else {
                    List<SlotRef> srcSlotRefList = Lists.newArrayList();
                    srcExpr.collect(SlotRef.class, srcSlotRefList);
                    result.addAll(srcSlotRefList.stream().map(e -> e.getSlotId()).collect(Collectors.toList()));
                }
            }
        }
        // conjunct
        List<SlotId> conjunctSlotIds = Lists.newArrayList();
        Expr.getIds(conjuncts, null, conjunctSlotIds);
        result.addAll(conjunctSlotIds);
        return result;
    }

    @Override
    protected Pair<Boolean, Boolean> needToCopyRightAndLeft() {
        boolean copyleft = true;
        boolean copyRight = true;
        switch (joinOp) {
            case LEFT_ANTI_JOIN:
            case LEFT_SEMI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                copyRight = false;
                break;
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                copyleft = false;
                break;
            default:
                break;
        }
        return Pair.of(copyleft, copyRight);
    }

    /**
     * Only for Nereids.
     */
    public CrossJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, List<TupleId> tupleIds,
            List<Expr> srcToOutputList, TupleDescriptor intermediateTuple, TupleDescriptor outputTuple) {
        super(id, "CROSS JOIN", StatisticalType.CROSS_JOIN_NODE, JoinOperator.CROSS_JOIN);
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
        LOG.debug("stats CrossJoin: cardinality={}", Long.toString(cardinality));
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).addValue(super.debugString()).toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.nested_loop_join_node = new TNestedLoopJoinNode();
        msg.nested_loop_join_node.join_op = joinOp.toThrift();
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
        msg.node_type = TPlanNodeType.CROSS_JOIN_NODE;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);

        // Only for Vec: create new tuple for join result
        if (VectorizedUtil.isVectorized()) {
            computeOutputTuple(analyzer);
        }
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        String distrModeStr = "";
        StringBuilder output =
                new StringBuilder().append(detailPrefix).append("join op: ").append(joinOp.toString()).append("(")
                        .append(distrModeStr).append(")\n");

        if (detailLevel == TExplainLevel.BRIEF) {
            output.append(detailPrefix).append(String.format("cardinality=%s", cardinality)).append("\n");
            return output.toString();
        }

        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("other predicates: ").append(getExplainString(conjuncts)).append("\n");
        }
        output.append(detailPrefix).append(String.format("cardinality=%s", cardinality)).append("\n");
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
