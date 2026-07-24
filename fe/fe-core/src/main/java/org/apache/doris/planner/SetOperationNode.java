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
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
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
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
    private static final Logger LOG = LoggerFactory.getLogger(SetOperationNode.class);

    // List of set operation result exprs of the originating SetOperationStmt. Used for
    // determining passthrough-compatibility of children.
    protected List<Expr> setOpResultExprs;

    // Materialized result/const exprs corresponding to materialized slots.
    // Set in finalize() and substituted against the corresponding child's output smap.
    protected List<List<Expr>> materializedResultExprLists = Lists.newArrayList();
    protected List<List<Expr>> materializedConstExprLists = Lists.newArrayList();

    // Index of the first non-passthrough child.
    protected int firstMaterializedChildIdx;

    protected final TupleId tupleId;

    private boolean isColocate = false;

    private DistributionMode distributionMode = DistributionMode.PARTITIONED;

    protected SetOperationNode(PlanNodeId id, TupleId tupleId, String planNodeName) {
        super(id, tupleId.asList(), planNodeName);
        this.setOpResultExprs = Lists.newArrayList();
        this.tupleId = tupleId;
    }

    public void setMaterializedConstExprLists(List<List<Expr>> exprs) {
        this.materializedConstExprLists = exprs;
    }

    public void setMaterializedResultExprLists(List<List<Expr>> exprs) {
        this.materializedResultExprLists = exprs;
    }

    public List<List<Expr>> getMaterializedResultExprLists() {
        return materializedResultExprLists;
    }

    public void setColocate(boolean colocate) {
        this.isColocate = colocate;
    }

    public DistributionMode getDistributionMode() {
        return distributionMode;
    }

    public void setDistributionMode(DistributionMode distributionMode) {
        this.distributionMode = distributionMode;
    }

    protected void toThrift(TPlanNode msg, TPlanNodeType nodeType) {
        Preconditions.checkState(materializedResultExprLists.size() == children.size());
        List<List<TExpr>> texprLists = Lists.newArrayList();
        for (List<Expr> exprList : materializedResultExprLists) {
            texprLists.add(ExprToThriftVisitor.treesToThrift(exprList));
        }
        List<List<TExpr>> constTexprLists = Lists.newArrayList();
        for (List<Expr> constTexprList : materializedConstExprLists) {
            constTexprLists.add(ExprToThriftVisitor.treesToThrift(constTexprList));
        }
        Preconditions.checkState(firstMaterializedChildIdx <= children.size());
        switch (nodeType) {
            case UNION_NODE:
                msg.union_node = new TUnionNode(
                        tupleId.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx);
                msg.node_type = TPlanNodeType.UNION_NODE;
                break;
            case INTERSECT_NODE:
                msg.intersect_node = new TIntersectNode(
                        tupleId.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx);
                msg.intersect_node.setIsColocate(isColocate);
                msg.node_type = TPlanNodeType.INTERSECT_NODE;
                break;
            case EXCEPT_NODE:
                msg.except_node = new TExceptNode(
                        tupleId.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx);
                msg.except_node.setIsColocate(isColocate);
                msg.node_type = TPlanNodeType.EXCEPT_NODE;
                break;
            default:
                LOG.error("Node type: " + nodeType + " is invalid.");
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
        if (CollectionUtils.isNotEmpty(materializedConstExprLists)) {
            output.append(prefix).append("constant exprs: ").append("\n");
            for (List<Expr> exprs : materializedConstExprLists) {
                output.append(prefix).append("    ").append(exprs.stream()
                        .map(e -> e.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE))
                        .collect(Collectors.joining(" | "))).append("\n");
            }
        }
        if (detailLevel == TExplainLevel.VERBOSE) {
            if (CollectionUtils.isNotEmpty(materializedResultExprLists)) {
                output.append(prefix).append("child exprs: ").append("\n");
                for (List<Expr> exprs : materializedResultExprLists) {
                    output.append(prefix).append("    ").append(exprs.stream()
                            .map(e -> e.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE))
                            .collect(Collectors.joining(" | "))).append("\n");
                }
            }
            List<String> passThroughNodeIds = Lists.newArrayList();
            for (int i = 0; i < firstMaterializedChildIdx; ++i) {
                passThroughNodeIds.add(children.get(i).getId().toString());
            }
            if (!passThroughNodeIds.isEmpty()) {
                String result = prefix + "pass-through-operands: ";
                if (passThroughNodeIds.size() == children.size()) {
                    output.append(result).append("all\n");
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

    public boolean isBucketShuffle() {
        return distributionMode.equals(DistributionMode.BUCKET_SHUFFLE);
    }

    public boolean isColocate() {
        return isColocate;
    }

    @Override
    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(PlanTranslatorContext translatorContext,
            PlanNode parent, LocalExchangeTypeRequire parentRequire) {
        LocalExchangeTypeRequire requireChild;
        LocalExchangeType outputType;

        // One derivation for union / intersect / except. Only two things differ between them, and
        // both are carried by the conditions below instead of by separate per-operation paths:
        //
        //   1. whether the children have to be aligned at all — intersect/except compare rows
        //      across branches so they always do (requiresShuffleForCorrectness() = true), while a
        //      union is a plain concatenation that only does when a downstream operator depends on
        //      the hash distribution. That mirrors BE's UnionSinkOperatorX, which returns
        //      GLOBAL_HASH(_distribute_exprs) only when _followed_by_shuffled_operator=true; the
        //      flag reaches us through enforceRequire. See PlanNode.requiresShuffleForCorrectness()
        //      for a chain-propagation example.
        //   2. what to require when the children are not bucket aligned — a union imposes nothing
        //      of its own and relays its parent's requirement; intersect/except impose GLOBAL.
        //
        // The bucket-aligned case is deliberately a single branch shared by all three: it is the
        // one rule that must hold for every set operation, and giving each operation its own copy
        // is how union came to be left out of it.
        boolean needAlignedChildren = requiresShuffleForCorrectness()
                || translatorContext.hasShuffleForCorrectnessAncestor(this);

        if (!needAlignedChildren) {
            // Union whose distribution nobody depends on: constrain nothing.
            requireChild = LocalExchangeTypeRequire.noRequire();
            outputType = LocalExchangeType.NOOP;
        } else if (AddLocalExchange.isColocated(this) || isBucketShuffle()) {
            // COLOCATE / BUCKET_SHUFFLE: every child is distributed by the basic child's storage
            // bucket function (basic side scans buckets directly, other sides come from
            // bucket-shuffle exchanges), so all children must stay aligned by that bucket function
            // locally. requireBucketHash keeps bucket-distributed children as-is and re-aligns a
            // serial (NOOP-claim) child with a BUCKET_HASH_SHUFFLE local exchange — same pattern as
            // HashJoinNode's colocate/bucket-shuffle branch.
            //
            // A generic requireHash() cannot express this: satisfy() accepts BUCKET_HASH_SHUFFLE,
            // LOCAL_ and GLOBAL_EXECUTION_HASH_SHUFFLE interchangeably and is evaluated per child,
            // so children may settle on *different* hash functions and still each report the
            // require as met. A child arriving on a bucket-shuffle exchange then keeps its
            // bucket->instance placement while a child that provides no hash gets a freshly
            // inserted LOCAL_EXECUTION_HASH_SHUFFLE, and the same key lands on two different local
            // tasks — whoever asked for the shuffle (an analytic PARTITION BY on the set operation
            // key, or the intersect/except itself) then sees a split partition.
            requireChild = LocalExchangeTypeRequire.requireBucketHash();
            outputType = LocalExchangeType.BUCKET_HASH_SHUFFLE;
        } else if (this instanceof UnionNode) {
            // Union has no requirement of its own: forward the parent's and report the resolved
            // type upward.
            requireChild = parentRequire.autoRequireHash();
            outputType = AddLocalExchange.resolveExchangeType(requireChild);
        } else {
            // PARTITIONED intersect/except: all children enter via global hash exchange. Require
            // GLOBAL so any inserted exchange matches the cross-fragment instance mapping (same
            // reason as HashJoinNode). Exception: serial source → fall back to LOCAL.
            boolean serialSource = fragment != null
                    && fragment.useSerialSource(translatorContext.getConnectContext());
            requireChild = serialSource
                    ? LocalExchangeTypeRequire.requireHash()
                    : LocalExchangeTypeRequire.requireGlobalExecutionHash();
            outputType = AddLocalExchange.resolveExchangeType(requireChild);
        }

        ArrayList<PlanNode> newChildren = Lists.newArrayList();
        for (int i = 0; i < children.size(); i++) {
            newChildren.add(enforceRequire(translatorContext, children.get(i), i, requireChild).first);
        }
        this.children = newChildren;
        return Pair.of(this, outputType);
    }

    @Override
    protected boolean shouldResetSerialFlagForChild(int childIndex) {
        return true;
    }
}
