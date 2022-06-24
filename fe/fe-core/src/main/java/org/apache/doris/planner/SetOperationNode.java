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
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.CheckedMath;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.thrift.TExceptNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TIntersectNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TUnionNode;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node that merges the results of its child plans, Normally, this is done by
 * materializing the corresponding result exprs into a new tuple. However, if
 * a child has an identical tuple layout as the output of the set operation node, and
 * the child only has naked SlotRefs as result exprs, then the child is marked
 * as 'passthrough'. The rows of passthrough children are directly returned by
 * the set operation node, instead of materializing the child's result exprs into new
 * tuples.
 */
public abstract class SetOperationNode extends PlanNode {
    private final static Logger LOG = LoggerFactory.getLogger(SetOperationNode.class);

    // List of set operation result exprs of the originating SetOperationStmt. Used for
    // determining passthrough-compatibility of children.
    protected List<Expr> setOpResultExprs_;

    // Expr lists corresponding to the input query stmts.
    // The ith resultExprList belongs to the ith child.
    // All exprs are resolved to base tables.
    protected List<List<Expr>> resultExprLists_ = Lists.newArrayList();

    // Expr lists that originate from constant select stmts.
    // We keep them separate from the regular expr lists to avoid null children.
    protected List<List<Expr>> constExprLists_ = Lists.newArrayList();

    // Materialized result/const exprs corresponding to materialized slots.
    // Set in finalize() and substituted against the corresponding child's output smap.
    protected List<List<Expr>> materializedResultExprLists_ = Lists.newArrayList();
    protected List<List<Expr>> materializedConstExprLists_ = Lists.newArrayList();

    // Indicates if this UnionNode is inside a subplan.
    protected boolean isInSubplan_;

    // Index of the first non-passthrough child.
    protected int firstMaterializedChildIdx_;

    protected final TupleId tupleId_;

    protected SetOperationNode(PlanNodeId id, TupleId tupleId, String planNodeName) {
        super(id, tupleId.asList(), planNodeName);
        setOpResultExprs_ = Lists.newArrayList();
        tupleId_ = tupleId;
        isInSubplan_ = false;
    }

    protected SetOperationNode(PlanNodeId id, TupleId tupleId, String planNodeName,
                               List<Expr> setOpResultExprs, boolean isInSubplan) {
        super(id, tupleId.asList(), planNodeName);
        setOpResultExprs_ = setOpResultExprs;
        tupleId_ = tupleId;
        isInSubplan_ = isInSubplan;
    }

    public void addConstExprList(List<Expr> exprs) {
        constExprLists_.add(exprs);
    }

    /**
     * Returns true if this UnionNode has only constant exprs.
     */
    public boolean isConstantUnion() {
        return resultExprLists_.isEmpty();
    }

    /**
     * Add a child tree plus its corresponding unresolved resultExprs.
     */
    public void addChild(PlanNode node, List<Expr> resultExprs) {
        super.addChild(node);
        resultExprLists_.add(resultExprs);
    }

    public List<List<Expr>> getMaterializedResultExprLists_() {
        return materializedResultExprLists_;
    }

    public List<List<Expr>> getMaterializedConstExprLists_() {
        return materializedConstExprLists_;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        super.finalize(analyzer);
        // In Doris-6380, moved computePassthrough() and the materialized position of resultExprs/constExprs from this.init()
        // to this.finalize(), and will not call SetOperationNode::init() again at the end of createSetOperationNodeFragment().
        //
        // Reasons for move computePassthrough():
        //      Because the byteSize of the tuple corresponding to OlapScanNode is updated after
        //      singleNodePlanner.createSingleNodePlan() and before singleNodePlan.finalize(),
        //      calling computePassthrough() in SetOperationNode::init() may not be able to accurately determine whether
        //      the child is pass through. In the previous logic , Will call SetOperationNode::init() again
        //      at the end of createSetOperationNodeFragment().
        //
        // Reasons for move materialized position of resultExprs/constExprs:
        //      Because the output slot is materialized at various positions in the planner stage, this is to ensure that
        //      eventually the resultExprs/constExprs and the corresponding output slot have the same materialized state.
        //      And the order of materialized resultExprs must be the same as the order of child adjusted by
        //      computePassthrough(), so resultExprs materialized must be placed after computePassthrough().

        // except Node must not reorder the child
        if (!(this instanceof ExceptNode)) {
            computePassthrough(analyzer);
        }
        // drop resultExprs/constExprs that aren't getting materialized (= where the
        // corresponding output slot isn't being materialized)
        materializedResultExprLists_.clear();
        Preconditions.checkState(resultExprLists_.size() == children.size());
        List<SlotDescriptor> slots = analyzer.getDescTbl().getTupleDesc(tupleId_).getSlots();
        for (int i = 0; i < resultExprLists_.size(); ++i) {
            List<Expr> exprList = resultExprLists_.get(i);
            List<Expr> newExprList = Lists.newArrayList();
            Preconditions.checkState(exprList.size() == slots.size());
            for (int j = 0; j < exprList.size(); ++j) {
                if (slots.get(j).isMaterialized()) {
                    newExprList.add(exprList.get(j));
                }
            }
            materializedResultExprLists_.add(
                    Expr.substituteList(newExprList, getChild(i).getOutputSmap(), analyzer, true));
        }
        Preconditions.checkState(
                materializedResultExprLists_.size() == getChildren().size());

        materializedConstExprLists_.clear();
        for (List<Expr> exprList : constExprLists_) {
            Preconditions.checkState(exprList.size() == slots.size());
            List<Expr> newExprList = Lists.newArrayList();
            for (int i = 0; i < exprList.size(); ++i) {
                if (slots.get(i).isMaterialized()) {
                    newExprList.add(exprList.get(i));
                }
            }
            materializedConstExprLists_.add(newExprList);
        }
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        if (!analyzer.safeIsEnableJoinReorderBasedCost()) {
            return;
        }
        computeCardinality();
    }

    @Override
    protected void computeOldCardinality() {
        computeCardinality();
    }

    private void computeCardinality() {
        cardinality = constExprLists_.size();
        for (PlanNode child : children) {
            // ignore missing child cardinality info in the hope it won't matter enough
            // to change the planning outcome
            if (child.cardinality > 0) {
                cardinality = CheckedMath.checkedAdd(cardinality, child.cardinality);
            }
        }
        // The number of nodes of a set operation node is -1 (invalid) if all the referenced tables
        // are inline views (e.g. select 1 FROM (VALUES(1 x, 1 y)) a FULL OUTER JOIN
        // (VALUES(1 x, 1 y)) b ON (a.x = b.y)). We need to set the correct value.
        if (numNodes == -1) {
            numNodes = 1;
        }
        capCardinalityAtLimit();
        if (LOG.isDebugEnabled()) {
            LOG.trace("stats Union: cardinality=" + cardinality);
        }
    }

    /**
     * Returns true if rows from the child with 'childTupleIds' and 'childResultExprs' can
     * be returned directly by the set operation node (without materialization into a new tuple).
     */
    private boolean isChildPassthrough(
            Analyzer analyzer, PlanNode childNode, List<Expr> childExprList) {
        List<TupleId> childTupleIds = childNode.getTupleIds();
        // Check that if the child outputs a single tuple, then it's not nullable. Tuple
        // nullability can be considered to be part of the physical row layout.
        Preconditions.checkState(childTupleIds.size() != 1 ||
                !childNode.getNullableTupleIds().contains(childTupleIds.get(0)));
        // If the Union node is inside a subplan, passthrough should be disabled to avoid
        // performance issues by forcing tiny batches.
        // TODO: Remove this as part of IMPALA-4179.
        if (isInSubplan_) {
            return false;
        }
        // Pass through is only done for the simple case where the row has a single tuple. One
        // of the motivations for this is that the output of a UnionNode is a row with a
        // single tuple.
        if (childTupleIds.size() != 1) {
            return false;
        }
        Preconditions.checkState(!setOpResultExprs_.isEmpty());

        TupleDescriptor setOpTupleDescriptor = analyzer.getDescTbl().getTupleDesc(tupleId_);
        TupleDescriptor childTupleDescriptor =
                analyzer.getDescTbl().getTupleDesc(childTupleIds.get(0));

        // Verify that the set operation tuple descriptor has one slot for every expression.
        Preconditions.checkState(setOpTupleDescriptor.getSlots().size() == setOpResultExprs_.size());
        // Verify that the set operation node has one slot for every child expression.
        Preconditions.checkState(
                setOpTupleDescriptor.getSlots().size() == childExprList.size());

        if (setOpResultExprs_.size() != childTupleDescriptor.getSlots().size()) {
            return false;
        }
        if (setOpTupleDescriptor.getByteSize() != childTupleDescriptor.getByteSize()) {
            return false;
        }

        for (int i = 0; i < setOpResultExprs_.size(); ++i) {
            if (!setOpTupleDescriptor.getSlots().get(i).isMaterialized())
                continue;
            SlotRef setOpSlotRef = setOpResultExprs_.get(i).unwrapSlotRef(false);
            SlotRef childSlotRef = childExprList.get(i).unwrapSlotRef(false);
            Preconditions.checkNotNull(setOpSlotRef);
            if (childSlotRef == null) {
                return false;
            }
            if (VectorizedUtil.isVectorized()) {
                // On vectorized engine, we have more chance to do passthrough.
                if (childSlotRef.getDesc().getSlotOffset() != setOpSlotRef.getDesc().getSlotOffset()) {
                    return false;
                }
                if (childSlotRef.isNullable() != setOpSlotRef.isNullable()) {
                    return false;
                }
                if (childSlotRef.getDesc().getType() != setOpSlotRef.getDesc().getType()) {
                    return false;
                }
            } else {
                if (!childSlotRef.getDesc().LayoutEquals(setOpSlotRef.getDesc())) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Compute which children are passthrough and reorder them such that the passthrough
     * children come before the children that need to be materialized. Also reorder
     * 'resultExprLists_'. The children are reordered to simplify the implementation in the
     * BE.
     */
    void computePassthrough(Analyzer analyzer) {
        List<List<Expr>> newResultExprLists = Lists.newArrayList();
        ArrayList<PlanNode> newChildren = Lists.newArrayList();
        for (int i = 0; i < children.size(); i++) {
            if (isChildPassthrough(analyzer, children.get(i), resultExprLists_.get(i))) {
                newResultExprLists.add(resultExprLists_.get(i));
                newChildren.add(children.get(i));
            }
        }
        firstMaterializedChildIdx_ = newChildren.size();

        for (int i = 0; i < children.size(); i++) {
            if (!isChildPassthrough(analyzer, children.get(i), resultExprLists_.get(i))) {
                newResultExprLists.add(resultExprLists_.get(i));
                newChildren.add(children.get(i));
            }
        }

        Preconditions.checkState(resultExprLists_.size() == newResultExprLists.size());
        resultExprLists_ = newResultExprLists;
        Preconditions.checkState(children.size() == newChildren.size());
        children = newChildren;
    }

    /**
     * Must be called after addChild()/addConstExprList(). Computes the materialized
     * result/const expr lists based on the materialized slots of this UnionNode's
     * produced tuple. The UnionNode doesn't need an smap: like a ScanNode, it
     * materializes an original tuple.
     * There is no need to call assignConjuncts() because all non-constant conjuncts
     * have already been assigned to the set operation operands, and all constant conjuncts have
     * been evaluated during registration to set analyzer.hasEmptyResultSet_.
     */
    @Override
    public void init(Analyzer analyzer) throws UserException {
        Preconditions.checkState(conjuncts.isEmpty());
        createDefaultSmap(analyzer);
        computeTupleStatAndMemLayout(analyzer);
        computeStats(analyzer);
    }

    protected void toThrift(TPlanNode msg, TPlanNodeType nodeType) {
        Preconditions.checkState( materializedResultExprLists_.size() == children.size());
        List<List<TExpr>> texprLists = Lists.newArrayList();
        for (List<Expr> exprList : materializedResultExprLists_) {
            texprLists.add(Expr.treesToThrift(exprList));
        }
        List<List<TExpr>> constTexprLists = Lists.newArrayList();
        for (List<Expr> constTexprList : materializedConstExprLists_) {
            constTexprLists.add(Expr.treesToThrift(constTexprList));
        }
        Preconditions.checkState(firstMaterializedChildIdx_ <= children.size());
        switch (nodeType) {
            case UNION_NODE:
                msg.union_node = new TUnionNode(
                        tupleId_.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx_);
                msg.node_type = TPlanNodeType.UNION_NODE;
                break;
            case INTERSECT_NODE:
                msg.intersect_node = new TIntersectNode(
                        tupleId_.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx_);
                msg.node_type = TPlanNodeType.INTERSECT_NODE;
                break;
            case EXCEPT_NODE:
                msg.except_node = new TExceptNode(
                        tupleId_.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx_);
                msg.node_type = TPlanNodeType.EXCEPT_NODE;
                break;
            default:
                LOG.error("Node type: " + nodeType.toString() + " is invalid.");
                break;
        }
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        if (detailLevel == TExplainLevel.BRIEF) {
            return "";
        }

        StringBuilder output = new StringBuilder();
        // A SetOperationNode may have predicates if a union is set operation inside an inline view,
        // and the enclosing select stmt has predicates referring to the inline view.
        if (CollectionUtils.isNotEmpty(conjuncts)) {
            output.append(prefix).append("predicates: ").append(getExplainString(conjuncts)).append("\n");
        }
        if (CollectionUtils.isNotEmpty(constExprLists_)) {
            output.append(prefix).append("constant exprs: ").append("\n");
            for (List<Expr> exprs : constExprLists_) {
                output.append(prefix).append("    ").append(exprs.stream().map(Expr::toSql)
                        .collect(Collectors.joining(" | "))).append("\n");
            }
        }
        if (detailLevel == TExplainLevel.VERBOSE) {
            if (CollectionUtils.isNotEmpty(materializedResultExprLists_)) {
                output.append(prefix).append("child exprs: ").append("\n");
                for(List<Expr> exprs : materializedResultExprLists_) {
                    output.append(prefix).append("    ").append(exprs.stream().map(Expr::toSql)
                            .collect(Collectors.joining(" | "))).append("\n");
                }
            }
            List<String> passThroughNodeIds = Lists.newArrayList();
            for (int i = 0; i < firstMaterializedChildIdx_; ++i) {
                passThroughNodeIds.add(children.get(i).getId().toString());
            }
            if (!passThroughNodeIds.isEmpty()) {
                String result = prefix + "pass-through-operands: ";
                if (passThroughNodeIds.size() == children.size()) {
                    output.append(result + "all\n");
                } else {
                    output.append(result).append(Joiner.on(",").join(passThroughNodeIds)).append("\n");
                }
            }
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        int numInstances = 0;
        for (PlanNode child : children) {
            numInstances += child.getNumInstances();
        }
        numInstances = Math.max(1, numInstances);
        return numInstances;
    }
}
